## ---------------------------
##
## Script name: Build_Monitoring_Site_Layer.R
##
## Purpose of script: Gather, transform, and merge data files showing the location of monitoring sites.
##                    Then join sites to their catalog entry to bring in metadata
##                    Exported to ESRI compatible format for display in ArcGIS online
##
## Author: Dr.François-Nicolas Robinne, Data/Habitat analyst
##
## Date Created: 2024-04-09
##
## Email: frobinne@psf.ca
##
## ---------------------------
##
## Notes:
##  Several datasets are not available online to date and were loaded from local drive
##  This script is subject to regular updates as source files locating monitoring sites change or are added
##  Errors might occur if source file formats change
##  Total time to run the full script with 13th Gen Intel(R) Core(TM) i9-13900H 2.60 GHz/3.20GB RAM == 16.97 seconds
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
library(datastreamr) # access to DataStream API (see https://github.com/datastreamapp/datastreamr)
library(ckanr) # access to CKAN-based open data repositories
library(tictoc) # compute run time for script execution

## Load functions -------------------------------------------------------

# This function helps create a quick dynamic map to check if the output is well located
spat_check <- function(sf_file) {
  check <- leaflet::leaflet(sf_file) %>%
    leaflet::addProviderTiles("CartoDB.Positron") %>%
    leaflet::addCircleMarkers()
  return(check)
}

## Load data catalogue  -------------------------------------------------
# might need authentication
catalog <- read_sheet("https://docs.google.com/spreadsheets/d/1vUXUDR4I9Ufw11jGbk4nVzgi7mdzj2k0-Oy3Eg8mw4Y/edit?pli=1#gid=0",
                      sheet = 3) # Goes to sheet 3 containing the actual sources and dataset unique IDs

## Start timer ----------------------------------------------------------
tic("Total run time") # Start run time recording

# Load API tokens ---------------------------------------------------------
ds_api_token <- read_lines("03_CODE_ANALYSIS/Freshwater_Monitoring_Catalogue/api_key.txt",
                           n_max = 1) # Reads DataStream API key stored locally

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
  unbc_code <- as.data.frame(str_extract_all(string = unbc_id, pattern = "\\([^()]+\\)")) # Extract code in parentheses
  unbc_code <- unbc_code %>%
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


# Prepare DFO CoSMO monitoring sites --------------------------------------
cosmo <- ds_locations(ds_api_token,
                      filter =  c("DOI='10.25976/0gvo-9d12'"),
                      select = c('Name', 'NameId', 'Latitude', 'Longitude')) %>%
    rename(site_name = Name, site_uid = NameId, latitude = Latitude, longitude = Longitude) %>%
    mutate(dataset_unique_identifier = "SWP_DTS_A022")
  
  cosmo_sf <- st_as_sf(cosmo, crs = 4326, coords = c("longitude", "latitude"))
  

# Prepare Resilient Waters monitoring sites -------------------------------
res_waters <- ds_locations(ds_api_token,
                           filter = c("DOI='10.25976/vdu8-o597"),
                           select = c('Name', 'NameId', 'Latitude', 'Longitude')) %>%
    rename(site_name = Name, site_uid = NameId, latitude = Latitude, longitude = Longitude) %>%
    mutate(dataset_unique_identifier = "SWP_DTS_A002")

  res_waters_sf <- st_as_sf(res_waters, crs = 4326, coords = c("longitude", "latitude"))
  

# Prepare Peninsula Streams Society monitoring sites ----------------------
pss <- ds_locations(ds_api_token,
                    filter = c("DOI='10.25976/v87k-2m08"),
                    select = c('Name', 'NameId', 'Latitude', 'Longitude')) %>%
    rename(site_name = Name, site_uid = NameId, latitude = Latitude, longitude = Longitude) %>%
    mutate(dataset_unique_identifier = "SWP_DTS_A153")

  pss_sf <- st_as_sf(pss, crs = 4326, coords = c('longitude', 'latitude'))
  
# Prepare Sunshine Coast Streamkeepers monitoring sites -------------------
# Waiting for the dataset to be available on DataStream


# Prepare DFO/SKT Upper Bulkley monitoring sites --------------------------
skt_url <- "https://maps.skeenasalmon.info/geoserver/ows?service=WFS&version=1.0.0&request=GetFeature&typename=geonode%3Amonitoring_sites_ubr_ubr_2018_10_02&outputFormat=csv&srs=EPSG%3A4326"
  
  # Store in temporary dir/file (only available during session)
  skt_temp_dir <- tempdir() 
  skt_temp_file <- tempfile(tmpdir = skt_temp_dir, fileext = ".csv")
  
  download.file(skt_url, skt_temp_file)
  
  skt_ub <- read_csv(skt_temp_file) %>%
    filter(ORGANIZATI == "Upper Bulkley Roundtable") %>%
    select(FID, STATION_NA, LATITUDE, LONGITUDE) %>%
    rename(site_name = STATION_NA, site_uid = FID, latitude = LATITUDE, longitude = LONGITUDE) %>%
    mutate(dataset_unique_identifier = "SWP_DTS_A003")
  
  skt_ub_sf <- st_as_sf(skt_ub, crs = 4326, coords = c('longitude', 'latitude'))
    

# Prepare Salmo WSS monitoring sites --------------------------------------
# Connects to Columbia Basin Data Hub
cbdh <- ckanr_setup("https://data.cbwaterhub.ca/") # No need for a key here
  
  # Erie creek (upper)
  # Get the CKAN resource
  swss_erie <- resource_show(id = "ce7f6bb3-f7c1-4d55-9ee3-4380a88c9ceb",
                             as = "table")
  # Download the file into the current session
  fetch_swss_erie <- ckan_fetch(swss_erie$url, "session", format = ".csv")
  # Format file
  swss_erie_site <- fetch_swss_erie %>%
    first() %>% # Only need the first record to get coordinates
    select(site_id, latitude, longitude) %>%
    mutate(site_uid = "swss_1") %>%
    rename(site_name = site_id) %>%
    relocate(site_uid, site_name, latitude, longitude) %>%
    mutate(dataset_unique_identifier = "SWP_DTS_A057")
  
  swss_erie_site_sf <- st_as_sf(swss_erie_site, crs = 4326, coords = c('longitude', 'latitude'))
  
  # Clearwater creek (upper and lower)
  # Get the CKAN resource
  swss_clearwater <- resource_show(id = "c1c79fea-c60b-4e96-8b2f-d722852a47f8",
                                   as = "table")
  # Download the file into the current session
  fetch_swss_clearwater <- ckan_fetch(swss_clearwater$url, "session", format = ".csv")
  # Format file
  swss_clearwater_site <- fetch_swss_clearwater %>%
    group_by(site_id) %>% # group by site name (upper and lower)
    slice(1) %>% # keep only the first record of each group
    select(site_id, latitude, longitude) %>%
    mutate(site_uid = case_when(
      site_id == "Clearwater_Lower" ~ "swss_2",
      site_id == "Clearwater_Upper" ~ "swss_3")) %>%
    rename(site_name = site_id) %>%
    relocate(site_uid, site_name, latitude, longitude) %>%
    mutate(dataset_unique_identifier = "SWP_DTS_A061")
    
  swss_clearwater_site_sf <- st_as_sf(swss_clearwater_site, crs = 4326, coords = c('longitude', 'latitude'))
  
  # Qua creek (upper and lower)
  # Get the CKAN resource
  swss_qua <- resource_show(id = "25c58dfa-d337-461b-8da4-1ba54ca62b55",
                            as = "table")
  # Download the file into the current session
  fetch_swss_qua <- ckan_fetch(swss_qua$url, "session", format = ".csv")
  # Format file
  swss_qua_site <- fetch_swss_qua %>%
    group_by(site_id) %>% # group by site name (upper and lower)
    slice(1) %>% # keep only the first record of each group
    select(site_id, latitude, longitude) %>%
    mutate(site_uid = case_when(
      site_id == "Qua_Lower" ~ "swss_4",
      site_id == "Qua_Upper" ~ "swss_5"
    )) %>%
    rename(site_name = site_id) %>%
    relocate(site_uid, site_name, latitude, longitude) %>%
    mutate(dataset_unique_identifier = "SWP_DTS_A058")
  
  swss_qua_site_sf <- st_as_sf(swss_qua_site, crs = 4326, coords = c('longitude', 'latitude'))
  
  # Hidden creek (upper and lower)
  # Get the CKAN resource
  swss_hidden <- resource_show(id = "9fad1c52-5226-40a3-ad63-468439c160cd",
                            as = "table")
  # Download the file into the current session
  fetch_swss_hidden <- ckan_fetch(swss_hidden$url, "session", format = ".csv")
  # Format file
  swss_hidden_site <- fetch_swss_hidden %>%
    group_by(site_id) %>% # group by site name (upper and lower)
    slice(1) %>% # keep only the first record of each group
    filter(site_id != "Hidden_upperNCC") %>% # Filter out the name with lower case (same value)
    select(site_id, latitude, longitude) %>%
    mutate(site_uid = case_when(
      site_id == "Hidden_Lower" ~ "swss_6",
      site_id == "Hidden_UpperNCC" ~ "swss_7"
    )) %>%
    rename(site_name = site_id) %>%
    relocate(site_uid, site_name, latitude, longitude) %>%
    mutate(dataset_unique_identifier = "SWP_DTS_A059")
  
  swss_hidden_site_sf <- st_as_sf(swss_hidden_site, crs = 4326, coords = c('longitude', 'latitude'))

  # Curtis creek (upper and lower)
  # Get the CKAN resource
  swss_curtis <- resource_show(id = "8e170fd6-b2be-4b72-8ae4-30279fb3c9d9",
                               as = "table")
  # Download the file into the current session
  fetch_swss_curtis <- ckan_fetch(swss_curtis$url, "session", format = ".csv")
  # Format file
  swss_curtis_site <- fetch_swss_curtis %>%
    group_by(site_id) %>% # group by site name (upper and lower)
    slice(1) %>% # keep only the first record of each group
    select(site_id, latitude, longitude) %>%
    mutate(site_uid = case_when(
      site_id == "Curtis_Lower" ~ "swss_8",
      site_id == "Curtis_Upper" ~ "swss_9"
    )) %>%
    rename(site_name = site_id) %>%
    relocate(site_uid, site_name, latitude, longitude) %>%
    mutate(dataset_unique_identifier = "SWP_DTS_A060")
  
  swss_curtis_site_sf <- st_as_sf(swss_curtis_site, crs = 4326, coords = c('longitude', 'latitude'))
  
  # South Salmo creek (upper and lower)
  # Get the CKAN resource
  swss_salmo <- resource_show(id = "670fdb95-58e0-400f-943f-2ab602fd659e",
                               as = "table")
  # Download the file into the current session
  fetch_swss_salmo <- ckan_fetch(swss_salmo$url, "session", format = ".csv")
  # Format file
  swss_salmo_site <- fetch_swss_salmo %>%
    group_by(site_id) %>% # group by site name (upper and lower)
    slice(1) %>% # keep only the first record of each group
    select(site_id, latitude, longitude) %>%
    mutate(site_uid = case_when(
      site_id == "SSR_Lower" ~ "swss_10",
      site_id == "SSR_Upper" ~ "swss_11"
    )) %>%
    rename(site_name = site_id) %>%
    relocate(site_uid, site_name, latitude, longitude) %>%
    mutate(dataset_unique_identifier = "SWP_DTS_A104")
  
  swss_salmo_site_sf <- st_as_sf(swss_salmo_site, crs = 4326, coords = c('longitude', 'latitude'))
  
  # Sheep creek (upper and lower)
  # Get the CKAN resource
  swss_sheep <- resource_show(id = "a3507ee6-72a0-4b74-80cf-14048bcafcf7",
                              as = "table")
  # Download the file into the current session
  fetch_swss_sheep <- ckan_fetch(swss_sheep$url, "session", format = ".csv")
  # Format file
  swss_sheep_site <- fetch_swss_sheep %>%
    group_by(site_id) %>% # group by site name (upper and lower)
    slice(1) %>% # keep only the first record of each group
    select(site_id, latitude, longitude) %>%
    mutate(site_uid = case_when(
      site_id == "Sheep_Lower" ~ "swss_12",
      site_id == "Sheep_Upper" ~ "swss_13"
    )) %>%
    rename(site_name = site_id) %>%
    relocate(site_uid, site_name, latitude, longitude) %>%
    mutate(longitude = str_trim(longitude, side = "left")) %>% 
    mutate(dataset_unique_identifier = "SWP_DTS_A134")
  
  swss_sheep_site_sf <- st_as_sf(swss_sheep_site, crs = 4326, coords = c('longitude', 'latitude'))
  
  # Merge all SWSS datasets together
  swss_sites_sf <- bind_rows(swss_sheep_site_sf, swss_salmo_site_sf, swss_curtis_site_sf,
                             swss_hidden_site_sf, swss_qua_site_sf, swss_clearwater_site_sf,
                             swss_erie_site_sf)

# Prepare DFO Pacfish sites -----------------------------------------------
# Note that these spatial locations are not accurate and were retrieved from the website's map
# Check map here: http://www.pacfish.ca/wcviweather/
pacfish_sites <- st_read("02_PROCESSED_DATA/DFO_PacFish/Hydromets.shp")
  
  # Format sites
  pacfish_sf <- pacfish_sites %>%
    st_transform(crs = 4326) %>%
    mutate(site_uid = paste0("pacfish_", id)) %>%
    select(-id) %>%
    rename(site_name = SITE_NAME) %>%
    mutate(dataset_unique_identifier = "SWP_DTS_A154")


# Prepare Kitasoo monitoring sites ----------------------------------------

  
# Create monitoring point layer -------------------------------------------
# Take all spatial layers created above and merge them
  
site_catalogue_sf <- bind_rows(bc_compiled_sf, hakai_sf,
                               unbc_sf, cosmo_sf, res_waters_sf,
                               pss_sf, skt_ub_sf, pacfish_sf, swss_sites_sf) %>%
    left_join(catalog) %>% # This should automatically use the field "dataset_unique_identifier"
    select(-c(Id, comments, date_dts_pse, date_dts_catalog, data_sharing_agreement, data_acquired))

# Save catalogue layer
st_write(site_catalogue_sf, dsn = "02_PROCESSED_DATA/PSF/catalogue_monitoring_sites.gpkg", delete_dsn = T)

# End timer --------------------------------------------------------------
toc()
         