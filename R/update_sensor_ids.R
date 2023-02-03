#' Function to update the \code{`sites`}'s \code{sensor_ids} variable for a 
#' sensors \strong{smonitor} database. 
#' 
#' @author Stuart K. Grange
#' 
#' @param con Database connection to an sensors \strong{smonitor} database. 
#' 
#' @return Invisible \code{con}. 
#' 
#' @export
update_sensor_ids <- function(con) {
  
  # Check if sites and sensors tables exist
  stopifnot(databaser::db_table_exists(con, c("sites", "sensors")))
  
  # Get and collapse sensor ids
  df <- databaser::db_get(
    con, 
    "SELECT DISTINCT 
    site,
    sensor_id
    FROM processes
    ORDER BY site"
  ) %>% 
    group_by(site) %>% 
    summarise(sensor_ids = stringr::str_c(sort(unique(sensor_id)), collapse = "; ")) %>% 
    ungroup()
  
  # Update database
  # Set sensor_ids to null first
  databaser::db_execute(con, "UPDATE sites SET sensor_ids = NULL")
  
  # Update sensor_ids
  df %>% 
    databaser::build_update_statements("sites", ., where = "site") %>% 
    databaser::db_execute(con, .)
  
  return(invisible(con))
  
}
