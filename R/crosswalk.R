get_crosswalk_definition <- function(
  entity = character(),
  config_path = env_conf_crosswalk_file(),
  config_id = "krypton_0.0.1",
  verbose = FALSE
) {
  cw <- if (length(entity)) {
    confx::conf_get(entity, from = config_path, config = config_id)
  } else {
    confx::conf_get(from = config_path, config = config_id) %>%
      # TODO-20201112-1116: Fix this further upstream so that `placeholder`
      # isn't even captured from the YAML (which it shouldn't as we explicitly
      # specified config = xy)
      `[`(which(!(names(.) %in% "placeholder")))
  }

  cw <- cw %>%
    purrr::map_dfr(function(.x) {
      # browser()
      # Ensure that nested lists are transformed to tibbles (only works for depth = 1)
      # TODO-20201112-1145: Generalize transformation of nested lists in arbitrary depths
      list_index <- .x %>% purrr::map_lgl(~.x %>% inherits("list"))
      if (any(list_index)) {
        # .x[list_index] <- .x[list_index] %>%
        #   purrr::map(~.x %>% dplyr::bind_rows())
        .x[list_index] <- .x[list_index] %>%
          purrr::map(~.x %>%
              dplyr::bind_rows() %>%
              list()
          )
      }

      # .x %>% dplyr::bind_rows()
      .x %>% tibble::as_tibble()
    })

  .resolve_references <- function(cw) {
    cw %>%
      dplyr::rowwise() %>%
      dplyr::group_split() %>%
      purrr::map_dfr(function(.row) {
        if (verbose) {
          # print(.row)
          message(.row$original)
        }
        reference_index <- .row %>%
          purrr::map_lgl(function(.x) {
            # Only check for references when value is a string (e.g. to not
            # apply the logic to list columns)
            ifelse(
              # Check for NA first
              is.na(.x),
              FALSE,
              # Then do the actual reference check
              ifelse(
                inherits(.x, "character"),
                stringr::str_detect(.x, "^/") %>%
                  all(),
                FALSE
              )
            )
          })

        cols_with_refs <- reference_index[reference_index] %>%
          names()

        if (length(cols_with_refs)) {
          .row %>%
            dplyr::relocate(internal, external, .after = original) %>%
            dplyr::mutate(
              dplyr::across(
                cols_with_refs,
                function(.x) {
                  # browser()
                  col_ref <- .x %>%
                    unique() %>%
                    stringr::str_remove("/")

                  stopifnot(length(col_ref) == 1)

                  col_ref <- col_ref %>% dplyr::sym()
                  dplyr::cur_data()[[col_ref]]
                }
              )
            )
        } else {
          .row
        }
      })
  }

  cw %>%
    .resolve_references() %>%
    .resolve_references() %>%
    # TODO-20201112-1210: That's still a hack to apply the logic twice. It should
    # be applied as many times as necessary --> implement a recursive logic

    # Post-process 'value' list column
    {
      if ("values" %in% names(.)) {
        dplyr::mutate(
          .,
          values = values %>%
            purrr::map(~.x %>% .resolve_references())
        )
      } else {
        .
      }
    }
}

# Rename ------------------------------------------------------------------

#' Rename columns (generic)
#'
#' @param data
#' @param ...
#'
#' @return
#' @export
#'
#' @examples
rename_columns <- function(data, ...) {
  UseMethod("rename_columns", data)
}

#' Rename columns (`data_travex`)
#'
#' See [new_data_travex_base_enr]
#'
#' @param data
#' @param version
#' @param stage
#' @param .col_from
#' @param .col_to
#' @param .col_label
#'
#' @return
#' @export
#'
#' @examples
rename_columns.data_travex <- function(
  data,
  version = "v1",
  stage = "global",
  .col_from = valid_column_name_type("original"),
  .col_to = valid_column_name_type("external"),
  .col_label = valid_column_attributes("label")
) {
  # Input handling
  .col_from = rlang::sym(.col_from)
  .col_to = rlang::sym(.col_to)
  .col_label = rlang::sym(.col_label)

  # Legacy
  # crosswalk_def = conf_column_name(
  #   entity = conf_entity_travex_audit_overview(path_sub = "columns"),
  #   has_ref = TRUE
  # )
  # Keep as reference

  # Get crosswalk definition based on curried entity function
  cw_def <- conf_column_name(
    entity = factory_conf_entity(
      data,
      version = version,
      stage = stage
    )()
  )

  # Actual renaming
  res <- try(
    rlang::call2(
      crosswalkr::renamefrom,
      .data = data,
      cw_file = cw_def,
      raw = .col_from,
      clean = .col_to,
      label = .col_label,
      drop_extra = FALSE
    ) %>%
      rlang::eval_tidy(),
    silent = TRUE
  )
  # TODO: Fix multiple rename cycles. Must have something to do with reactivity

  if (inherits(res, "try-error")) {
    data
  } else {
    res
  }
}
