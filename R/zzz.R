#' Function to squash R check's global variable notes. 
#' 
#' @name zzz
#' 
if (getRversion() >= "2.15.1") {
  
  # What variables are causing issues?
  variables <- c(
    ".", "data_source", "date_analysed", "date_end", "date_start", 
    "description", "model", "observations", "period_exclude", "period_valid", 
    "process", "raster", "sampling_point_id", "sensing_element_id", 
    "sensor_group", "sensor_id", "sensor_type", "site", "site_name", 
    "valid", "variable", "variable_predict", "n_observations", "ws_min",
    "ws_max", "wd_min", "wd_max", "ws_range", "wd_range", "date_start_delta",
    "duration", "inlet", "interval", "sensor_type_name", "exclude", "period",
    "fill_sequence", "cylinder_id", "fill_sequence_pad", "cylinder_group",
    "date_type", "date_start_range", "date_end_range", "notes", "value"
  )
  
  # Squash the notes
  utils::globalVariables(variables)
  
}
