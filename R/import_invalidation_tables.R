#' Function to import invalidation data from the \code{`invalidations`} and
#' \code{`invalidations_date_ranges`} tables as a single table for a ssensors 
#' \strong{smonitor} database.
#' 
#' @author Stuart K. Grange
#' 
#' @param con Database connection to a ssensors \strong{smonitor} database. 
#' 
#' @param tz Time zone for the dates to be parsed into. Default is \code{"UTC"}. 
#' 
#' @return Tibble. 
#' 
#' @seealso \code{\link{import_invalidations}}, 
#' \code{\link{import_invalid_date_ranges}}
#' 
#' @export
import_invalidation_tables <- function(con, tz = "UTC") {
  
  # Import invalidations if it exists
  if (databaser::db_table_exists(con, "invalidations")) {
    df_invalidations <- databaser::db_get(
      con,
      "SELECT invalidations.*,
      processes.site,
      processes.variable,
      processes.sensor_id,
      'invalidations' AS 'table'
      FROM invalidations
      LEFT JOIN processes 
      ON invalidations.process = processes.process
      ORDER by process,
      date_start"
    )
  } else {
    df_invalidations <- tibble()
  }
  
  # Import invalidations_date_ranges if it exists
  if (databaser::db_table_exists(con, "invalidations_date_ranges")) {
    df_date_ranges <- databaser::db_get(
      con,
      "SELECT *,
      'invalidations_date_ranges' AS 'table'
      FROM invalidations_date_ranges
      ORDER by sensor_id,
      variable,
      date_start"
    ) %>% 
      mutate(date_end = as.numeric(date_end))
  } else {
    df_date_ranges <- tibble()
  }
  
  # Bind the two tables
  df <- df_invalidations %>% 
    bind_rows(df_date_ranges) 
  
  # Clean table up a bit
  if (nrow(df) != 0L) {
    df <- df %>% 
      mutate(
        across(c(date_start, date_end), ~threadr::parse_unix_time(., tz = tz))
      ) %>% 
      relocate(table,
               process,
               site,
               sensor_id,
               variable,
               date_start,
               date_end,
               data_source)
  }
  
  return(df)
  
}
