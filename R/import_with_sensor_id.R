#' Functions to import data from an extended sensors \strong{smonitor} database. 
#' 
#' @author Stuart K. Grange
#' 
#' @param con Database connection to an sensors \strong{smonitor} database. 
#' 
#' @param process A vector of processes. If a data frame/tibble is passed 
#' containing a variable called \code{process}, this vector will be extracted 
#' and used. 
#' 
#' @param summary A vector of summaries. 
#' 
#' @param start What is the start date of data to be returned? Ideally, the 
#' date format should be \code{yyyy-mm-dd}, but the UK locale convention of 
#' \code{dd/mm/yyyy} will also work. Years as strings or integers work too and
#' will floor-rounded. 
#' 
#' @param end What is the end date of data to be returned? Ideally, the 
#' date format should be \code{yyyy-mm-dd}, but the UK locale convention of 
#' \code{dd/mm/yyyy} will also work. Years as strings or integers work too and 
#' will be ceiling-rounded. 
#' 
#' @param site_name Should the return include the \code{site_name} variable? 
#' Default is \code{TRUE}. 
#' 
#' @param valid_only Should invalid observations be filtered out? Default is 
#' \code{TRUE}. Valid observations are considered to be those with the validity
#' variable being \code{1} or missing (\code{NULL} or \code{NA}). This argument
#' will be set to \code{FALSE} if \code{set_invalid_values} is used.
#' 
#' @param set_invalid_values Should invalid observations be set to \code{NA}? 
#' See \code{\link{set_invalid_values}} for details and this argument will set
#' the \code{valid_only} argument to \code{FALSE}. 
#' 
#' @param warn Should the functions raise warnings? 
#' 
#' @param add_extras Should extra variables be calculated and returned? 
#' 
#' @param tz Time-zone for the dates to be parsed into. Default is \code{"UTC"}. 
#' 
#' @return Tibble. 
#' 
#' @seealso \link{import_by_process} 
#' 
#' @export
import_with_sensor_id <- function(con, process, summary = NA, start = 1969, 
                                  end = NA, site_name = TRUE, valid_only = FALSE, 
                                  set_invalid_values = FALSE, warn = FALSE, 
                                  tz = "UTC") {
  
  # Check if the extra ssensors data model tables exist
  stopifnot(databaser::db_table_exists(con, c("processes", "sensors")))
  
  # If a data frame has been passed for the progress argument, pull the process
  # variable if it exists, the naming is not very helpful here! 
  if (is.data.frame(process) & "process" %in% names(process)) {
    process <- pull(process, process)
  }
  
  # Get extra sensor things from `processes`
  df_sensor_ids <- glue::glue(
    "SELECT processes.process,
    processes.sensor_id,
    processes.sensing_element_id,
    sensors.sensor_type
    FROM processes 
    LEFT JOIN sensors 
    ON processes.sensor_id = sensors.sensor_id
    WHERE process IN ({stringr::str_c(process, collapse = ',')})"
  ) %>% 
    databaser::db_get(con, .)
  
  # Get observations
  df <- smonitor::import_by_process(
    con, 
    process = process, 
    summary = summary,
    start = start,
    end = end,
    site_name = site_name,
    valid_only = valid_only,
    set_invalid_values = set_invalid_values,
    warn = warn,
    tz = tz
  )
  
  # If no observations
  if (nrow(df) == 0L) {
    return(tibble())
  }
  
  # Join extras to observations
  df <- df %>% 
    left_join(df_sensor_ids, by = join_by(process)) %>% 
    relocate(sensor_id,
             sensing_element_id,
             sensor_type,
             .after = variable)
  
  return(df)
  
}


#' @rdname import_with_sensor_id
#' @export
import_reference_cylinders <- function(con, tz = "UTC") {
  
  stopifnot(databaser::db_table_exists(con, "reference_cylinders"))
  
  databaser::db_get(
    con, 
    "SELECT * 
    FROM reference_cylinders
    ORDER BY cylinder_id,
    statistic,
    date_analysed"
  ) %>% 
    mutate(
      across(
        c(date_analysed, date_start, date_end), 
        ~threadr::parse_unix_time(as.numeric(.), tz = tz)
      )
    )
  
}


#' @rdname import_with_sensor_id
#' @export
import_cylinder_deployments <- function(con, tz = "UTC") {
  
  # Check if the tables exist
  stopifnot(
    databaser::db_table_exists(con, c("deployments_cylinders", "sensors"))
  )
  
  databaser::db_get(
    con,
    "SELECT deployments_cylinders.*,
    sensors.sensor_type
    FROM deployments_cylinders
    LEFT JOIN sensors
    ON deployments_cylinders.sensor_id = sensors.sensor_id
    ORDER BY sensor_id,
    date_start"
  ) %>% 
    mutate(
      across(
        c(date_start, date_end), 
        ~threadr::parse_unix_time(as.numeric(.), tz = tz)
      ),
      interval = lubridate::interval(date_start, date_end)
    ) %>% 
    relocate(sensor_id,
             sensor_type) %>% 
    relocate(interval,
             .after = date_end)
  
}


#' @rdname import_with_sensor_id
#' @export
import_observations_calibrations <- function(con, tz = "UTC") {
  
  stopifnot(databaser::db_table_exists(con, "observations_calibrations"))
  
  databaser::db_get(
    con,
    "SELECT observations_calibrations.*,
    processes.sensor_id,
    processes.site,
    sites.site_name
    FROM observations_calibrations
    LEFT JOIN processes 
    ON observations_calibrations.process = processes.process
    LEFT JOIN sites
    ON processes.site = sites.site
    ORDER BY 
    process, 
    inlet,
    date"
  ) %>% 
    relocate(process,
             sensor_id,
             site,
             site_name) %>% 
    mutate(
      across(
        c(date_start, date_end, date), 
        ~threadr::parse_unix_time(as.numeric(.), tz = tz)
      )
    )
  
}


#' @rdname import_with_sensor_id
#' @export
import_cylinder_test_summaries <- function(con, tz = "UTC") {
  
  # Check if table exists
  stopifnot(databaser::db_table_exists(con, "cylinder_test_summaries"))
  
  # Get data and format some data-types
  databaser::db_get(
    con,
    "SELECT *
    FROM cylinder_test_summaries
    ORDER BY sensor_id,
    variable,
    date_start,
    inlet"
  ) %>% 
    mutate(
      across(
        c(date_start, date_end), 
        ~threadr::parse_unix_time(as.numeric(.), tz = tz)
      ),
      duration = threadr::parse_time(duration),
      across(c(period_exclude, period_valid), as.logical)
    ) %>% 
    group_by(sensor_id,
             sensing_element_id,
             inlet,
             variable) %>% 
    mutate(date_start_delta = threadr::lag_delta(as.numeric(date_start))) %>% 
    ungroup() %>% 
    mutate(date_start_delta = threadr::parse_time(date_start_delta))
  
}


#' @rdname import_with_sensor_id
#' @export
import_sensor_deployments <- function(con, tz = "UTC") {
  
  stopifnot(databaser::db_table_exists(con, "deployments_sensors"))
  
  databaser::db_get(
    con, 
    "SELECT deployments_sensors.*,
    sites.site_name,
    sensors.sensor_type
    FROM deployments_sensors
    LEFT JOIN sites
    ON deployments_sensors.site = sites.site
    LEFT JOIN sensors
    ON deployments_sensors.sensor_id = sensors.sensor_id
    ORDER BY sensor_id,
    date_start"
  ) %>% 
    mutate(
      across(
        c(date_start, date_end),
        ~threadr::parse_unix_time(as.numeric(., tz = tz))
      ),
      interval = lubridate::interval(date_start, date_end)
    ) %>% 
    relocate(sensor_id,
             sensor_type,
             site,
             site_name) %>% 
    relocate(interval,
             .after = date_end)
  
}


#' @rdname import_with_sensor_id
#' @export
import_calibration_coefficients <- function(con, tz = "UTC") {
  
  # Check if tables exist
  stopifnot(
    databaser::db_table_exists(con, c("calibration_coefficients", "sensors"))
  )
  
  databaser::db_get(
    con,
    "SELECT cc.*
    FROM calibration_coefficients AS cc
    ORDER BY data_source,
    sensor_id,
    variable,
    date,
    term"
  ) %>% 
    mutate(date = threadr::parse_unix_time(date, tz = tz))
  
}


#' @rdname import_with_sensor_id
#' @export
import_sensing_elements <- function(con, tz = "UTC") {
  
  stopifnot(databaser::db_table_exists(con, "sensing_elements"))
  
  databaser::db_get(
    con, 
    "SELECT sensing_elements.*,
    sensors.sensor_type
    FROM sensing_elements
    LEFT JOIN sensors
    ON sensing_elements.sensor_id = sensors.sensor_id
    ORDER BY sensor_id,
    variable,
    date_start"
  ) %>% 
    mutate(
      across(
        c(date_start, date_end), 
        ~threadr::parse_unix_time(as.numeric(.), tz = tz)
      )
    ) %>% 
    relocate(sensor_type,
             .after = sensor_id)
  
}


#' @rdname import_with_sensor_id
#' @export
import_observation_flags <- function(con) {
  
  stopifnot(databaser::db_table_exists(con, "observation_flags"))
  
  databaser::db_get(
    con,
    "SELECT * 
    FROM observation_flags
    ORDER BY flag"
  )
  
}


#' @rdname import_with_sensor_id
#' @export
import_cylinder_exclusion_periods <- function(con, tz = "UTC") {
  
  stopifnot(databaser::db_table_exists(con, "cylinder_exclusion_periods"))
  
  databaser::db_get(
    con,
    "SELECT * 
    FROM cylinder_exclusion_periods
    ORDER BY sensor_id,
    date_start"
  ) %>% 
    mutate(
      across(c(date_start, date_end), ~threadr::parse_unix_time(., tz = tz))
    )
  
}


#' @rdname import_with_sensor_id
#' @export
import_sensors <- function(con) {
  
  # Check if table exists
  stopifnot(databaser::db_table_exists(con, "sensors"))
  
  # Get data
  databaser::db_get(
    con, 
    "SELECT sensors.*,
    sensor_types.sensor_group
    FROM sensors 
    LEFT JOIN sensor_types
    ON sensors.sensor_type = sensor_types.sensor_type
    ORDER BY sensor_id"
  ) %>% 
    relocate(sensor_id,
             sensor_type,
             sensor_group)
  
}


#' @rdname import_with_sensor_id
#' @export
import_sensor_types <- function(con) {
  
  # Check if table exists
  stopifnot(databaser::db_table_exists(con, "sensor_types"))
  
  # Get data
  databaser::db_get(
    con,
    "SELECT * 
    FROM sensor_types
    ORDER by primary_purpose,
    sensor_group,
    sensor_type"
  )
  
}


#' @rdname import_with_sensor_id
#' @export
import_invalid_date_ranges <- function(con, tz = "UTC") {
  
  stopifnot(databaser::db_table_exists(con, "invalidations_date_ranges"))
  
  databaser::db_get(
    con, 
    "SELECT * 
    FROM invalidations_date_ranges
    ORDER BY sensor_id,
    variable,
    date_start"
  ) %>% 
    mutate(across(c(date_start, date_end), ~threadr::parse_unix_time(., tz = tz)))
  
}


#' @rdname import_with_sensor_id
#' @export
import_validity_types <- function(con) {
  
  stopifnot(databaser::db_table_exists(con, "validity_types"))
  
  databaser::db_get(
    con, 
    "SELECT * 
    FROM validity_types 
    ORDER BY valid DESC"
  ) %>% 
    mutate(valid = as.logical(valid))
  
}


#' @rdname import_with_sensor_id
#' @export
import_model_objects <- function(con, add_extras = TRUE) {
  
  # Get blob from database
  df_blob <- databaser::db_get(
    con, 
    "SELECT blob 
    FROM r_objects 
    WHERE file = 'model_objects'"
  )
  
  # Decompress blob and make an R object again
  df_nest <- df_blob$blob[[1]] %>% 
    memDecompress(type = "gzip") %>% 
    unserialize()
  
  # Format a few things
  df_nest <- df_nest %>% 
    rowwise(data_source,
            site,
            sensor_id,
            variable,
            variable_predict)
  
  # Add a few extra things
  if (add_extras) {
    df_nest <- df_nest %>% 
      mutate(model_glance = list(broom::glance(model)),
             model_tidy = list(broom::tidy(model)))
  }
  
  return(df_nest)
  
}


#' @rdname import_with_sensor_id
#' @export
import_raster_objects <- function(con, tz = "UTC") {
  
  # Check if table exists
  stopifnot(databaser::db_table_exists(con, "raster_objects"))
  
  # Get data from database
  df <- databaser::db_get(
    con,
    "SELECT * 
    FROM raster_objects
    ORDER BY data_source,
    date,
    variable"
  ) %>% 
    mutate(date = threadr::parse_unix_time(date, tz = tz))
  
  # Parse the raw vectors/blobs into R objects
  df <- df %>% 
    rowwise(data_source,
            date,
            variable) %>% 
    mutate(observations = list(threadr::unserialise_r_object(observations, "gzip")),
           raster = list(threadr::unserialise_r_object(raster, "gzip")),
           raster = list(terra::unwrap(raster)),
           n_observations = nrow(observations)) %>% 
    relocate(n_observations,
             .before = observations)
  
  return(df)
  
}


#' @rdname import_with_sensor_id
#' @export
import_observation_flagging_conditions <- function(con) {
  
  # Check if table exists
  stopifnot(databaser::db_table_exists(con, "observation_flagging_conditions"))
  
  databaser::db_get(
    con,
    "SELECT observation_flagging_conditions.*,
    sites.site_name
    FROM observation_flagging_conditions
    LEFT JOIN sites
    ON observation_flagging_conditions.site = sites.site
    ORDER BY site,
    variable"
  ) %>% 
    relocate(site_name,
             .after = site) %>% 
    mutate(ws_range = ws_max - ws_min,
           wd_range = wd_max - wd_min) %>% 
    relocate(ws_range,
             wd_range,
             .after = wd_max)
  
}


#' @rdname import_with_sensor_id
#' @export
import_generic_observations <- function(con, tz = "UTC") {
  
  # Check if table exists
  stopifnot(databaser::db_table_exists(con, "generic_observations"))
  
  # Query database
  databaser::db_get(
    con, 
    "SELECT * 
    FROM generic_observations
    ORDER BY date"
  ) %>% 
    mutate(date = threadr::parse_unix_time(date, tz = tz))
  
}


#' @rdname import_with_sensor_id
#' @export
import_icos_cities_variables <- function(con) {
  
  # Check if table exists
  stopifnot(databaser::db_table_exists(con, "icos_cities_variables"))
  
  # Get all data
  databaser::db_get(
    con, 
    "SELECT * 
    FROM icos_cities_variables
    ORDER BY variable"
  )
  
}
