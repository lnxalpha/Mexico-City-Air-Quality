# Load necessary libraries
library(rsinaica)
library(lubridate)
library(dplyr)

# station_names <- c("AJM", "TLA", "XAL", "NEZ", "SAG", "VIF", "LPR", "IZT", "GAM", "IMP", "CES",
#                 "PED", "SFE", "CCA", "MER", "CUA", "BJU", "COY", "LOM", "TLA", "NTS", "VCA", 
#                 "AZC", "PLA", "ATI", "SFE", "CHA", "TPN", "MPA", "UIZ")

# Define station IDs and parameters
station_ids <- c(242, 266, 271, 258, 260, 270, 253, 252, 302, 322, 317,
                 259, 108, 245, 256, 248, 300, 247, 255, 266, 331, 246, 
                 315, 332, 82, 108, 120, 336, 299, 268)
parameters <- c("PM2.5", "PM10", "O3", "NO2", "SO2", "CO")

# Set up date range for 5 years (from 2024 down to 2019)
start_date <- as.Date("2019-01-01")
end_date <- as.Date("2024-11-01")
output_file <- "air_quality_data.csv"

# Initialize first_write flag for the CSV
first_write <- TRUE

# Create a grid of all station-parameter combinations
station_param_grid <- expand.grid(station_id = station_ids, parameter = parameters, stringsAsFactors = FALSE)

# Loop through each month in reverse order
current_date <- end_date
while (current_date >= start_date) {
  
  # Define the monthly date range
  month_end <- current_date
  month_start <- max(current_date - months(1) + days(1), start_date)
  
  print(paste("Fetching data from", month_start, "to", month_end))
  
  # Initialize a list to collect data for the month
  monthly_data <- lapply(1:nrow(station_param_grid), function(i) {
    station_id <- station_param_grid$station_id[i]
    parameter <- station_param_grid$parameter[i]
    
    # Fetch data with error handling
    tryCatch({
      df <- sinaica_station_data(
        station_id = station_id,
        parameter = parameter,
        start_date = as.character(month_start),
        end_date = as.character(month_end),
        type = "Crude"
      )
      # Add metadata columns if data is retrieved
      if (!is.null(df) && nrow(df) > 0) {
        df$station_id <- station_id
        df$parameter <- parameter
      }
      return(df)
    }, error = function(e) {
      message("Error fetching data for station:", station_id, "parameter:", parameter, ":", e)
      return(NULL)
    })
  })
  
  # Combine data for all stations and parameters in the month
  combined_data <- do.call(rbind, monthly_data)
  
  # Write or append to CSV if data is available
  if (!is.null(combined_data) && nrow(combined_data) > 0) {
    if (first_write) {
      write.csv(combined_data, output_file, row.names = FALSE)
      first_write <- FALSE
    } else {
      write.table(combined_data, output_file, row.names = FALSE, col.names = FALSE, sep = ",", append = TRUE)
    }
  }
  
  # Move to the previous month
  current_date <- month_start - days(1)
}

print("Data download and CSV append complete.")
