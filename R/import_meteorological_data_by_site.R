#' Function to import meteorological data for a monitoring site from an 
#' extended sensors \strong{smonitor} database. 
#' 
#' @author Stuart K. Grange
#' 
#' @param con Database connection to an sensors \strong{smonitor} database. 
#' 
#' @param site A vector of sites. If the \code{`sites`} table has a 
#' \code{site_pair} variable, this will be used as a switch. 
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
#' @param variables What meteorological variables should be queried? 
#' 
#' @param valid_only Should invalid observations be filtered out? 
#' 
#' @param tz Time-zone for the dates to be parsed into. Default is \code{"UTC"}. 
#' 
#' @return Tibble. 
#' 
#' @export
import_meteorological_data_by_site <- function(con, site, summary = 1L, 
                                               start = 1969, end = NA,
                                               variables = c("air_temp", "rh", "wd", "ws"),
                                               valid_only = FALSE, tz = "UTC") {
  
  # Get sites table
  df_sites <- smonitor::import_sites(con)
  
  # Query each site iteratively
  purrr::map(
    sort(site), 
    ~import_sites_met_worker(
      con,
      site = .,
      df_sites = df_sites,
      start = start,
      end = end, 
      summary = summary,
      variables = variables,
      valid_only = valid_only,
      tz = tz
    )
  ) %>% 
    purrr::list_rbind()
  
}


import_sites_met_worker <- function(con, site, df_sites, start, end, summary, 
                                    variables, valid_only, tz) {
  
  # Filter the sites table
  df_site <- filter(df_sites, site == !!site)
  
  # Switch site if needed
  if (!is.na(df_site$site_pair)) {
    site <- df_site$site_pair
  }
  
  # Get and filter processes
  df_processes <- con %>% 
    smonitor::import_processes() %>% 
    filter(site %in% !!site,
           variable %in% !!variables)
  
  # Get observations, replace site if needed and join site name
  df <- import_with_sensor_id(
    con, 
    process = df_processes$process,
    summary = summary,
    start = start,
    end = end,
    site_name = FALSE,
    valid_only = valid_only,
    tz = tz
  ) %>% 
    select(-sensing_element_id,
           -process) %>% 
    mutate(site = df_site$site) %>% 
    left_join(select(df_site, site, site_name), by = join_by(site)) %>% 
    relocate(site_name,
             .after = site)
  
  return(df)
  
}
