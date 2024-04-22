## ---------------------------
##
## Script name: Build_Monitoring_Site_Layer.R
##
## Purpose of script: Gather, transform, and merge data files showing the location of monitoring sites.
##                    Then join sites to their catalog entry to bring in metadata
##                    Exported to ESRI compatible format for display in ArcGIS online
##
## Author: Dr. François-Nicolas Robinne
##
## Date Created: 2024-04-09
##
## Email: frobinne@psf.ca
##
## ---------------------------
##
## Notes:
##   This script is subject to regular updates as source files locating monitoring sites change or are added
##    Errors might occur if source file formats change
##
## ---------------------------

## set working directory 

setwd("C:/Users/frobinne/Salmon Watersheds Dropbox/François-Nicolas Robinne/X Drive/1_PROJECTS/1_Active/Stream Temperature/Data & Analysis")

## load up the packages we will need -------------------------------------

library(dplyr) # data manip
library(readr) # quick data read
library(sf) # spatial data manip
library(stringr) # character vector manip
library(googlesheets4) # access and load google sheets
library(measurements) # unit conversion
library(textclean) # clean text

## Load functions --------------------------------------------------------


## Load data catalogue  -------------------------------------------------
# might need authentication
catalog <- read_sheet("https://docs.google.com/spreadsheets/d/1vUXUDR4I9Ufw11jGbk4nVzgi7mdzj2k0-Oy3Eg8mw4Y/edit?pli=1#gid=0",
                      sheet = 2) # Goes to sheet 2 containing the actual sources and dataset unique IDs


# Prepare BC monitoring sites ----------------------------------------------
# Load existing data compilation based of WSC, PSC, and BC Gov datasets
bc_compiled <- read_csv("Code/stream-temp/output/stations.csv")
  
  # Reformat the dataset to add unique IDs from the catalog
  bc_compiled_uid <- bc_compiled %>%
    select(-c(6:10)) %>% #removes unnecessary columns
    mutate(dataset_unique_identifier = case_when(
      Source == "BC" ~ "SWP_DTS_A010",
      Source == "WSC" ~ "SWP_DTS_A004",
      Source == "PSC" ~ "SWP_DTS_A000"
    )) %>%
    # Rename eveything to lower case and get rid of the "Source" field (clutter)
    rename(
      site_uid = StationID,
      site_name = StreamName,
      latitude = Latitude,
      longitude = Longitude
      ) %>%
    select(-Source) %>%
    filter(site_name != "Taite Creek") # This will have to be fixed eventually when recreating the BC compilation
  # Create spatial layer
  bc_compiled_sf <- st_as_sf(bc_compiled_uid, crs = 4326, coords = c("longitude","latitude"))


# Prepare Hakai monitoring sites -----------------------------------------
# The site coordinates are not store withih the CKAN datasets
# The code will be updated when (if?) the locations are added to Hakai's CKAN platform
hakai <- read_csv("01_RAW_DATA/Hakai/kwakstationscoords_v2.csv")
  
  # Reformat the dataset 
  hakai_format <- hakai %>%
    mutate(dataset_unique_identifier = case_when(
      data_package == "Koeye River stream temperature, stage, and conductivity time-series version 2, Adjusted Koeye River stage and temperature from 2013 to 2021" ~ "SWP_DTS_A006",
      data_package == "Baseline Limnology of Lakes in the Kwakshua Watersheds of Calvert and Hecate Islands, BC. 2016-2019 v2.0" ~ "SWP_DTS_A007",
      is.na(data_package) ~ "SWP_DTS_A008",
      str_detect(data_package, "[^[:ascii:]]") == T ~ "SWP_DTS_A008" # Calvert Island package has non ASCII characters that are used to check the condition
      )) %>%
    mutate(site_name = stations,
           site_uid = site_name,
           latitude = lat,
           longitude = long) %>% # This mutate should be a rename; more logical
    select(-c(stations, stream_temp, stage, conductivity, data_package, comments, lat, long))
  
  hakai_sf <- st_as_sf(hakai_format, crs = 4326, coords = c("longitude", "latitude"))

# Prepare UNBC monitoring sites (Stephen Déry) ---------------------------
# From downloaded file for now, but should be from Zenodo URL and load in session
unbc_dery_metadata <- readLines("01_RAW_DATA/UNBC_Dery/NHG Data ReadMe.txt") # Read the file
unbc_id <- as.data.frame(unbc_dery_metadata[grep('^Metadata:.*', unbc_dery_metadata)-1]) # Extract the line with site name and code
unbc_loc <- as.data.frame(unbc_dery_metadata[grep('^Metadata:.*', unbc_dery_metadata)]) # Extract site details

  # Extract site name
  unbc_name <- unbc_id %>% 
    mutate(site_name = str_sub(unbc_id[,1], end = -8)) %>% # Remove the trailing code in parentheses
    select(site_name)
    
  # Extract site code
  unbc_code <- as.data.frame(str_extract_all(string = unbc_id, pattern = "\\([^()]+\\)")) %>% # Extract code in parentheses
    mutate(site_uid = unbc_code[,1]) %>% # Replace the nonsense name of the first column
    mutate(site_uid = str_replace_all(site_uid, "[()]", "")) %>% # Removes parentheses around the code
    select(site_uid)
  
  # Extract coordinates
  # Last two variables created —lat and lon—convert coordinates from degree min sec to decimal degrees
  unbc_coord <- unbc_loc %>%
    mutate(site_meta = unbc_loc[,1]) %>%
    mutate(coord = str_sub(site_meta, start = 12)) %>%
    mutate(coord = str_remove(coord, "W.*")) %>%
    mutate(coord = str_remove_all(coord, "[°'\"]")) %>% # Format coordinates for conversion
    mutate(latitude = conv_unit(str_trim(str_remove(coord, "N.*")), from = 'deg_min_sec', to = 'dec_deg')) %>% 
    mutate(longitude = str_c("-", conv_unit(str_extract(coord, "\\b[^,]+$"), from = 'deg_min_sec', to = 'dec_deg'))) %>%
    select(latitude, longitude)
  
  # Merge them all (and in darkness bind them)
  unbc_join <- bind_cols(unbc_code, unbc_name, unbc_coord) %>%
    mutate(dataset_unique_identifier = "SWP_DTS_A051")
  # Create spatial layer
  unbc_sf <- st_as_sf(unbc_join, crs = 4326, coords = c("longitude", "latitude"))
  

# Create monitoring point layer -------------------------------------------
# Take all spatial layers created above and merge them
  
site_catalogue_sf <- bind_rows(bc_compiled_sf, hakai_sf, unbc_sf) %>%
    left_join(catalog) %>% # This should automatically use the field "dataset_unique_identifier"
    select(-c(comments, date_dts_pse, date_dts_catalog, data_sharing_agreement, data_acquired))

# Save catalogue layer
st_write(site_catalogue_sf, dsn = "02_PROCESSED_DATA/PSF/catalogue_monitoring_sites.gpkg", delete_dsn = T)
         