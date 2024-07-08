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
##  To do:
##  - Create API access for Kluane Lake
##  - Add renv
##  - Merge datasets per provider
##  - Connect to distant repos (e.g., CKAN, DataStream) once at the beginning
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
library(readxl) # read XLS files
library(arcgisbinding) # work with ESRI file formats

## Load functions -------------------------------------------------------

# This function helps create a quick dynamic map to check if the output is well located
spat_check <- function(sf_file) {
  check <- leaflet::leaflet(sf_file) %>%
    leaflet::addProviderTiles("CartoDB.Positron") %>%
    leaflet::addCircleMarkers()
  return(check)
}
#############################
## 1) Load data catalogue  ##
#############################

# might need authentication
catalog <- read_sheet("https://docs.google.com/spreadsheets/d/1vUXUDR4I9Ufw11jGbk4nVzgi7mdzj2k0-Oy3Eg8mw4Y/edit?pli=1#gid=0",
                      sheet = 3) # Goes to sheet 3 containing the actual sources and dataset unique IDs

## Start timer ----------------------------------------------------------
tic("Total run time") # Start run time recording

# Load API tokens ---------------------------------------------------------
ds_api_token <- read_lines("03_CODE_ANALYSIS/Freshwater_Monitoring_Catalogue/api_key.txt",
                           n_max = 1) # Reads DataStream API key stored locally

#################################
## 2) Prepare monitoring sites ##
#################################

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
# Make sure that the api token is loaded
cosmo <- ds_locations(ds_api_token,
                      filter =  c("DOI='10.25976/0gvo-9d12'"),
                      select = c('Name', 'NameId', 'Latitude', 'Longitude')) %>%
    rename(site_name = Name, site_uid = NameId, latitude = Latitude, longitude = Longitude) %>%
    mutate(dataset_unique_identifier = "SWP_DTS_A022")
  
  cosmo_sf <- st_as_sf(cosmo, crs = 4326, coords = c("longitude", "latitude"))
  

# Prepare Sunshine Coast Streamkeepers monitoring sites -------------------
# Make sure that the api token is loaded
scsk <- ds_locations(ds_api_token,
                      filter =  c("DOI='10.25976/ze3e-xf34'"),
                      select = c('Name', 'NameId', 'Latitude', 'Longitude')) %>%
    rename(site_name = Name, site_uid = NameId, latitude = Latitude, longitude = Longitude) %>%
    mutate(dataset_unique_identifier = "SWP_DTS_A147")
  
  scsk_sf <- st_as_sf(scsk, crs = 4326, coords = c("longitude", "latitude"))
  
  
# Prepare Resilient Waters monitoring sites -------------------------------
# Make sure that the api token is loaded
res_waters <- ds_locations(ds_api_token,
                           filter = c("DOI='10.25976/vdu8-o597'"),
                           select = c('Name', 'NameId', 'Latitude', 'Longitude')) %>%
    rename(site_name = Name, site_uid = NameId, latitude = Latitude, longitude = Longitude) %>%
    mutate(dataset_unique_identifier = "SWP_DTS_A002")

  res_waters_sf <- st_as_sf(res_waters, crs = 4326, coords = c("longitude", "latitude"))
  

# Prepare Peninsula Streams Society monitoring sites ----------------------
# Make sure that the api token is loaded
pss <- ds_locations(ds_api_token,
                    filter = c("DOI='10.25976/v87k-2m08"),
                    select = c('Name', 'NameId', 'Latitude', 'Longitude')) %>%
    rename(site_name = Name, site_uid = NameId, latitude = Latitude, longitude = Longitude) %>%
    mutate(dataset_unique_identifier = "SWP_DTS_A153")

  pss_sf <- st_as_sf(pss, crs = 4326, coords = c('longitude', 'latitude'))


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


# Prepare Rossland Streamkeepers monitoring sites --------------------------------------
# Those seven sites below can be merged if necessary, but they are separate entries in the catalog
# Connects to Columbia Basin Data Hub
cbdh <- ckanr_setup("https://data.cbwaterhub.ca/") # No need for a key here  
  
  # Golpher Creek
  # Get the CKAN resource
  ross_golpher <- resource_show(id = "e90372be-88ed-40d5-a578-a2bb261d4da1",
                              as = "table")
  # Download the file into the current session
  fetch_ross_golpher <- ckan_fetch(ross_golpher$url, "session", format = ".csv")
  # Format file
  ross_golpher_site <- fetch_ross_golpher %>%
    group_by(site_id) %>% # group by site name (upper and lower)
    slice(1) %>% # keep only the first record of each group
    select(site_id, latitude, longitude) %>%
    rename(site_name = site_id) %>%
    mutate(site_uid = "ross_1") %>% 
    mutate(dataset_unique_identifier = "SWP_DTS_A052")
  
  ross_golpher_site_sf <- st_as_sf(ross_golpher_site, crs = 4326, coords = c('longitude', 'latitude'), remove = T)
  
  # Centennial wetland
  # Get the CKAN resource
  ross_cent <- resource_show(id = "cc57e1ce-6fef-4631-986f-27d34aae8a38",
                                as = "table")
  # Download the file into the current session
  fetch_ross_cent <- ckan_fetch(ross_cent$url, "session", format = ".csv")
  # Format file
  ross_cent_site <- fetch_ross_cent %>%
    group_by(site_id) %>% # group by site name (upper and lower)
    slice(1) %>% # keep only the first record of each group
    select(site_id, latitude, longitude) %>%
    rename(site_name = site_id) %>%
    mutate(site_uid = "ross_2") %>% 
    mutate(dataset_unique_identifier = "SWP_DTS_A053")
  
  ross_cent_site_sf <- st_as_sf(ross_cent_site, crs = 4326, coords = c('longitude', 'latitude'), remove = T)
  
  # Tiger Creek
  # Get the CKAN resource
  ross_tiger <- resource_show(id = "a1585c90-196b-4614-aa36-7047c856e3ff",
                             as = "table")
  # Download the file into the current session
  fetch_ross_tiger <- ckan_fetch(ross_tiger$url, "session", format = ".csv")
  # Format file
  ross_tiger_site <- fetch_ross_tiger %>%
    group_by(site_id) %>% # group by site name (upper and lower)
    slice(1) %>% # keep only the first record of each group
    select(site_id, latitude, longitude) %>%
    rename(site_name = site_id) %>%
    mutate(site_uid = "ross_3") %>% 
    mutate(dataset_unique_identifier = "SWP_DTS_A054")
  
  ross_tiger_site_sf <- st_as_sf(ross_tiger_site, crs = 4326, coords = c('longitude', 'latitude'), remove = T)
  
  # Milkranch Creek
  # Get the CKAN resource
  ross_milk <- resource_show(id = "c8c2707e-010f-4992-8af9-f632724b7c5b",
                              as = "table")
  # Download the file into the current session
  fetch_ross_milk <- ckan_fetch(ross_milk$url, "session", format = ".csv")
  # Format file
  ross_milk_site <- fetch_ross_milk %>%
    group_by(site_id) %>% # group by site name (upper and lower)
    slice(1) %>% # keep only the first record of each group
    select(site_id, latitude, longitude) %>%
    rename(site_name = site_id) %>%
    mutate(site_uid = "ross_4") %>% 
    mutate(dataset_unique_identifier = "SWP_DTS_A056")
  
  ross_milk_site_sf <- st_as_sf(ross_milk_site, crs = 4326, coords = c('longitude', 'latitude'), remove = T)
  
  # Topping Creek
  # Get the CKAN resource
  ross_topp <- resource_show(id = "adb3757d-93d0-44ac-8e58-5d0d9b787258",
                             as = "table")
  # Download the file into the current session
  fetch_ross_topp <- ckan_fetch(ross_topp$url, "session", format = ".csv")
  # Format file
  ross_topp_site <- fetch_ross_topp %>%
    group_by(site_id) %>% # group by site name (upper and lower)
    slice(1) %>% # keep only the first record of each group
    select(site_id, latitude, longitude) %>%
    rename(site_name = site_id) %>%
    mutate(site_uid = "ross_5") %>% 
    mutate(dataset_unique_identifier = "SWP_DTS_A062")
  
  ross_topp_site_sf <- st_as_sf(ross_topp_site, crs = 4326, coords = c('longitude', 'latitude'), remove = T)
  
  # Cemetery Creek
  # Get the CKAN resource
  ross_ceme <- resource_show(id = "68feb799-4825-49d9-a947-657a97d1c96f",
                             as = "table")
  # Download the file into the current session
  fetch_ross_ceme <- ckan_fetch(ross_ceme$url, "session", format = ".csv")
  # Format file
  ross_ceme_site <- fetch_ross_ceme %>%
    group_by(site_id) %>% # group by site name (upper and lower)
    slice(1) %>% # keep only the first record of each group
    select(site_id, latitude, longitude) %>%
    rename(site_name = site_id) %>%
    mutate(site_uid = "ross_6") %>% 
    mutate(dataset_unique_identifier = "SWP_DTS_A063")
  
  ross_ceme_site_sf <- st_as_sf(ross_ceme_site, crs = 4326, coords = c('longitude', 'latitude'), remove = T)
  
  # Warfield Creek
  # Get the CKAN resource
  ross_warf <- resource_show(id = "68feb799-4825-49d9-a947-657a97d1c96f",
                             as = "table")
  # Download the file into the current session
  fetch_ross_warf <- ckan_fetch(ross_warf$url, "session", format = ".csv")
  # Format file
  ross_warf_site <- fetch_ross_warf %>%
    group_by(site_id) %>% # group by site name (upper and lower)
    slice(1) %>% # keep only the first record of each group
    select(site_id, latitude, longitude) %>%
    rename(site_name = site_id) %>%
    mutate(site_uid = "ross_7") %>% 
    mutate(dataset_unique_identifier = "SWP_DTS_A064")
  
  ross_warf_site_sf <- st_as_sf(ross_warf_site, crs = 4326, coords = c('longitude', 'latitude'), remove = T)
  
  # Falaise Creek
  # Get the CKAN resource
  ross_fala <- resource_show(id = "8f5587ca-e5c3-40d5-bb6e-36b9f8bc246e",
                             as = "table")
  # Download the file into the current session
  fetch_ross_fala <- ckan_fetch(ross_fala$url, "session", format = ".csv")
  # Format file
  ross_fala_site <- fetch_ross_fala %>%
    group_by(site_id) %>% # group by site name (upper and lower)
    slice(1) %>% # keep only the first record of each group
    select(site_id, latitude, longitude) %>%
    rename(site_name = site_id) %>%
    mutate(site_uid = "ross_8") %>% 
    mutate(dataset_unique_identifier = "SWP_DTS_A065")
  
  ross_fala_site_sf <- st_as_sf(ross_fala_site, crs = 4326, coords = c('longitude', 'latitude'), remove = T)
  
  # Haley Creek
  # Get the CKAN resource
  ross_hale <- resource_show(id = "af5ff8fc-d6bf-4c0a-b6d4-6fc68df5a5ee",
                             as = "table")
  # Download the file into the current session
  fetch_ross_hale <- ckan_fetch(ross_hale$url, "session", format = ".csv")
  # Format file
  ross_hale_site <- fetch_ross_hale %>%
    group_by(site_id) %>% # group by site name (upper and lower)
    slice(1) %>% # keep only the first record of each group
    select(site_id, latitude, longitude) %>%
    rename(site_name = site_id) %>%
    mutate(site_uid = "ross_9") %>% 
    mutate(dataset_unique_identifier = "SWP_DTS_A066")
  
  ross_hale_site_sf <- st_as_sf(ross_hale_site, crs = 4326, coords = c('longitude', 'latitude'), remove = T)
  
  # ==> Problem with this resource; throws an error back
  # # Cambridge Creek
  # # Get the CKAN resource
  # ross_camb <- resource_show(id = "597be3c8-44b1-4032-8f7f-867659996e4e",
  #                             as = "table")
  # # Download the file into the current session
  # fetch_ross_camb <- ckan_fetch(ross_camb$url, "session", format = ".csv")
  # # Format file
  # ross_camb_site <- fetch_ross_camb %>%
  #   group_by(site_id) %>% # group by site name (upper and lower)
  #   slice(1) %>% # keep only the first record of each group
  #   select(site_id, latitude, longitude) %>%
  #   rename(site_name = site_id) %>%
  #   mutate(site_uid = "ross_4") %>% 
  #   mutate(dataset_unique_identifier = "SWP_DTS_A054")
  
  # ross_camb_site_sf <- st_as_sf(ross_camb_site, crs = 4326, coords = c('longitude', 'latitude'), remove = T)
  
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
# These sites were provided by the Nation
kitasoo <- read_csv("02_PROCESSED_DATA/CCIRA_Kitasoo/Locations_Hobo_Tidbit_Loggers.csv")
  
  # Format sites
  kitasoo_sites <- kitasoo %>%
    rename(site_name = System,
           latitude = Lat,
           longitude = Long) %>%
    mutate(site_uid = paste0("kit_", as.character(row_number()))) %>%
    mutate(dataset_unique_identifier = "SWP_DTS_A049")
  
  kitasoo_sites_sf <- st_as_sf(kitasoo_sites, crs = 4326, coords = c('longitude', 'latitude'))
  

# Prepare Skeena Fisheries Commission monitoring sites --------------------
GWA_SFC <- read_sf("01_RAW_DATA/Skeena_Fisheries/SFC and GWA Temp Site Metadata for PSF.csv")
  
  # Format sites
  GWA_SFC_sites <- GWA_SFC %>%
    select(SiteID, `UTM Zone`, `UTM Easting`, `UTM Northing`) %>%
    mutate(Easting = as.numeric(`UTM Easting`)) %>%
    mutate(Northing = as.numeric(`UTM Northing`)) %>%
    filter(!is.na(Northing)) %>%
    mutate(site_name =  SiteID) %>%
    rename(site_uid = SiteID) %>%
    mutate(dataset_unique_identifier = "SWP_DTS_A149")
  
  GWA_SFC_sites_sf <- st_as_sf(GWA_SFC_sites, 
                               crs = 32609, # UTM 9N
                               coords = c('Easting', 'Northing')) %>%
    select(-c('UTM Zone', 'UTM Easting', 'UTM Northing')) %>%
    st_transform(crs = 4326)


# Prepare Shuswap Fisheries Commission monitoring sites -------------------
shushwap <- read_sf("01_RAW_DATA/ShuswapFisheriesCommission/Shuswap_Tw_Monitoring_Sites.shp")
  
  # Format sites
  shushwap_sf <- shushwap %>%
    select(Name) %>%
    rename(site_name = Name) %>%
    mutate(site_uid = paste0("shushwap_", as.character(row_number()))) %>%
    mutate(dataset_unique_identifier = "SWP_DTS_A155")
    

# Prepare Salmon Watershed Lab (SFU) monitoring sites ---------------------
sfu_swl <- read_csv("01_RAW_DATA/SalmonWatershedLab_SFU/nicola_sites_for_PSF.csv")

  # Format sites
  sfu_swl_sites <- sfu_swl %>%
    mutate(site_name = location,
           site_uid = paste0(site_operator, "_", site),
           latitude = str_sub(lat, end = -3), # Decrease precision of location to prevent vandalism
           longitude = str_sub(long, end = -3), # Decrease precision of location to prevent vandalism
           dataset_unique_identifier = "SWP_DTS_A148") %>%
    select(site_id, site_name, latitude, longitude, dataset_unique_identifier)
  
  sfu_swl_sites_sf <- st_as_sf(sfu_swl_sites, crs = 4326, coords = c("longitude", "latitude"))
  

# Prepare Dan Moore's UBC monitoring sites --------------------------------
ubc_moore <- read_csv("01_RAW_DATA/UBC_DanMoore/sites_coords_elevs.csv")

  # Format sites
  ubc_moore_sites <- ubc_moore %>%
    mutate(site_uid = id,
           site_name = id,
           dataset_unique_identifier = "SWP_DTS_A150") %>%
    select(site_id, site_name, lat, lon, dataset_unique_identifier)
  
  ubc_moore_sites_sf <- st_as_sf(ubc_moore_sites, crs = 4326, coords = c("lon", "lat"))

# Prepare Living Lakes High Elevation Monitoring Program monitoring sites ------
# Those seven sites below can be merged if necessary, but they are separate entries in the catalog
# Connects to Columbia Basin Data Hub
cbdh <- ckanr_setup("https://data.cbwaterhub.ca/") # No need for a key here
  
  # Upper Joker Lake
  # Get the CKAN resource
  llc_ujl <- resource_show(id = "c1971837-4beb-4284-8b4e-e1b1dae6d2e1", # Upper Joker Lake 
                               as = "table")
  # Download the file into the current session
  fetch_llc_ujl <- ckan_fetch(llc_ujl$url, "session", format = ".csv")
  # Format file
  llc_ujl_site <- fetch_llc_ujl %>%
    first() %>% # Only need the first record to get coordinates
    select(site_id, latitude, longitude) %>%
    mutate(site_uid = "llc_hemp_1") %>% # llc for Living Lakes Canada; nk for North Kootenay
    rename(site_name = site_id) %>%
    relocate(site_uid, site_name, latitude, longitude) %>%
    mutate(dataset_unique_identifier = "SWP_DTS_A108")
  
  llc_ujl_site_sf <- st_as_sf(llc_ujl_site, crs = 4326, coords = c('longitude', 'latitude'))
  
  # Sapphire Lake
  # Get the CKAN resource
  llc_sap <- resource_show(id = "35b7c6ab-2997-4238-9005-1adca8d31c6c", # Sapphire Lake
                           as = "table")
  # Download the file into the current session
  fetch_llc_sap <- ckan_fetch(llc_sap$url, "session", format = ".csv")
  # Format file
  llc_sap_site <- fetch_llc_sap %>%
    first() %>% # Only need the first record to get coordinates
    select(site_id, latitude, longitude) %>%
    mutate(site_uid = "llc_hemp_2") %>% # llc for Living Lakes Canada; nk for North Kootenay
    rename(site_name = site_id) %>%
    relocate(site_uid, site_name, latitude, longitude) %>%
    mutate(dataset_unique_identifier = "SWP_DTS_A132")
  
  llc_sap_site_sf <- st_as_sf(llc_sap_site, crs = 4326, coords = c('longitude', 'latitude'))
  
# Prepare Living Lakes North Kootenay Lake monitoring sites ------------------
# Those seven sites below can be merged if necessary, but they are separate entries in the catalog
# Connects to Columbia Basin Data Hub
cbdh <- ckanr_setup("https://data.cbwaterhub.ca/") # No need for a key here
  
  # Carlyle Creek
  # Get the CKAN resource
  llc_carlyle <- resource_show(id = "7a396917-63b9-4bcc-8d77-67fedbd9a109",
                             as = "table")
  # Download the file into the current session
  fetch_llc_carlyle <- ckan_fetch(llc_carlyle$url, "session", format = ".csv")
  # Format file
  llc_carlyle_site <- fetch_llc_carlyle %>%
    first() %>% # Only need the first record to get coordinates
    select(site_id, latitude, longitude) %>%
    mutate(site_uid = "llc_nk_1") %>% # llc for Living Lakes Canada; nk for North Kootenay
    rename(site_name = site_id) %>%
    relocate(site_uid, site_name, latitude, longitude) %>%
    mutate(dataset_unique_identifier = "SWP_DTS_A116")
  
  llc_carlyle_site_sf <- st_as_sf(llc_carlyle_site, crs = 4326, coords = c('longitude', 'latitude'))
  
  # Gar Creek
  # Get the CKAN resource
  llc_gar <- resource_show(id = "1cb02e19-2586-4196-a553-5daecfea7df1",
                               as = "table")
  # Download the file into the current session
  fetch_llc_gar <- ckan_fetch(llc_gar$url, "session", format = ".csv")
  # Format file
  llc_gar_site <- fetch_llc_gar %>%
    first() %>% # Only need the first record to get coordinates
    select(site_id, latitude, longitude) %>%
    mutate(site_uid = "llc_nk_2") %>% # llc for Living Lakes Canada; nk for North Kootenay
    rename(site_name = site_id) %>%
    relocate(site_uid, site_name, latitude, longitude) %>%
    mutate(dataset_unique_identifier = "SWP_DTS_A135")
  
  llc_gar_site_sf <- st_as_sf(llc_gar_site, crs = 4326, coords = c('longitude', 'latitude'))
  
  # MacDonald Creek
  # Get the CKAN resource
  llc_mcdo <- resource_show(id = "a8f43c04-2def-42f7-874b-f04910c5edef",
                           as = "table")
  # Download the file into the current session
  fetch_llc_mcdo <- ckan_fetch(llc_mcdo$url, "session", format = ".csv")
  # Format file
  llc_mcdo_site <- fetch_llc_mcdo %>%
    first() %>% # Only need the first record to get coordinates
    select(site_id, latitude, longitude) %>%
    mutate(site_uid = "llc_nk_3") %>% # llc for Living Lakes Canada; nk for North Kootenay
    rename(site_name = site_id) %>%
    relocate(site_uid, site_name, latitude, longitude) %>%
    mutate(dataset_unique_identifier = "SWP_DTS_A141")
  
  llc_mcdo_site_sf <- st_as_sf(llc_mcdo_site, crs = 4326, coords = c('longitude', 'latitude'))
  
  # Davis Creek
  # Get the CKAN resource
  llc_davis <- resource_show(id = "e19aff69-7004-49fc-9cf8-8e88f86a6563",
                            as = "table")
  # Download the file into the current session
  fetch_llc_davis <- ckan_fetch(llc_davis$url, "session", format = ".csv")
  # Format file
  llc_davis_site <- fetch_llc_davis %>%
    first() %>% # Only need the first record to get coordinates
    select(site_id, latitude, longitude) %>%
    mutate(site_uid = "llc_nk_4") %>% # llc for Living Lakes Canada; nk for North Kootenay
    rename(site_name = site_id) %>%
    relocate(site_uid, site_name, latitude, longitude) %>%
    mutate(dataset_unique_identifier = "SWP_DTS_A143")
  
  llc_davis_site_sf <- st_as_sf(llc_davis_site, crs = 4326, coords = c('longitude', 'latitude'))
  
  # Bjerkness Creek
  # Get the CKAN resource
  llc_bjerk <- resource_show(id = "56287ff2-f3da-4c6b-a407-e12061f14309",
                             as = "table")
  # Download the file into the current session
  fetch_llc_bjerk <- ckan_fetch(llc_bjerk$url, "session", format = ".csv")
  # Format file
  llc_bjerk_site <- fetch_llc_bjerk %>%
    first() %>% # Only need the first record to get coordinates
    select(site_id, latitude, longitude) %>%
    mutate(site_uid = "llc_nk_5") %>% # llc for Living Lakes Canada; nk for North Kootenay
    rename(site_name = site_id) %>%
    relocate(site_uid, site_name, latitude, longitude) %>%
    mutate(dataset_unique_identifier = "SWP_DTS_A144")
  
  llc_bjerk_site_sf <- st_as_sf(llc_bjerk_site, crs = 4326, coords = c('longitude', 'latitude'))
  
  # Kootenay Joe Creek
  # Get the CKAN resource
  llc_koot <- resource_show(id = "5983a159-759b-4524-82d4-ba2d0506ae6f",
                             as = "table")
  # Download the file into the current session
  fetch_llc_koot <- ckan_fetch(llc_koot$url, "session", format = ".csv")
  # Format file
  llc_koot_site <- fetch_llc_koot %>%
    first() %>% # Only need the first record to get coordinates
    select(site_id, latitude, longitude) %>%
    mutate(site_uid = "llc_nk_6") %>% # llc for Living Lakes Canada; nk for North Kootenay
    rename(site_name = site_id) %>%
    relocate(site_uid, site_name, latitude, longitude) %>%
    mutate(dataset_unique_identifier = "SWP_DTS_A145")
  
  llc_koot_site_sf <- st_as_sf(llc_koot_site, crs = 4326, coords = c('longitude', 'latitude'))
 
  # Ben Hur Creek
  # Get the CKAN resource
  llc_benh <- resource_show(id = "65788ebf-db78-4ed4-86ca-7fa9b189d0d3",
                            as = "table")
  # Download the file into the current session
  fetch_llc_benh <- ckan_fetch(llc_benh$url, "session", format = ".csv")
  # Format file
  llc_benh_site <- fetch_llc_benh %>%
    first() %>% # Only need the first record to get coordinates
    select(site_id, latitude, longitude) %>%
    mutate(site_uid = "llc_nk_7") %>% # llc for Living Lakes Canada; nk for North Kootenay
    rename(site_name = site_id) %>%
    relocate(site_uid, site_name, latitude, longitude) %>%
    mutate(dataset_unique_identifier = "SWP_DTS_A146")
  
  llc_benh_site_sf <- st_as_sf(llc_benh_site, crs = 4326, coords = c('longitude', 'latitude')) 

# Prepare Kluane Lake monitoring sites ------------------------------------
# Read downloaded XLSX file
# API access through Dataverse is possible (https://borealisdata.ca/guides/en/latest/api/index.html)
# Although this dataset might not be updated anymore, but would be nice for any other user
# Skipped first rows and last row containing metadata
uofa_kluane <- read_xlsx("01_RAW_DATA/UofA/KluaneMoorings_RawData.xlsx",
                         col_names = c("Mooring name", "Mooring location", "Empty", "Depth_Column"),
                         trim_ws = T,
                         n_max = 4,
                         skip = 5)
  
  # Format sites
  uofa_kluane_sites <- uofa_kluane %>%
    mutate(site_name = `Mooring name`,
           site_uid = paste0("klu_", as.character(row_number())),
           latitude = as.numeric(str_split_i(`Mooring location`, ",", i = 1)),
           longitude = as.numeric(str_split_i(`Mooring location`, ",", i = 2)),
           dataset_unique_identifier = "SWP_DTS_A047") %>%
    select(-c(`Mooring name`, `Mooring location`, Empty, Depth_Column))
  
  uofa_kluane_sites_sf <- st_as_sf(uofa_kluane_sites, crs = 4326, coords = c("longitude", "latitude"))


# Prepare UNBC Quesnel River monitoring sites -----------------------------
# Data initially provided in a MS Word document. 
# Faster and easier to process the seven sites manually first to create a CSV file to import
unbc_quesnel <- read_csv("02_PROCESSED_DATA/UNBC_QUESNEL/Site_Locations.csv")
    
  # Format sites
  unbc_quesnel_sites <- unbc_quesnel %>%
    mutate(site_name = Name,
           site_uid = ID,
           dataset_unique_identifier = "SWP_DTS_A048") %>%
    select(-c(Name, ID))
  
  unbc_quesnel_sites_sf <- st_as_sf(unbc_quesnel_sites, crs = 4326, coords = c("Lon", "Lat"))

  
# Prepare BCGOV Bevington monitoring sites --------------------------------
bcgov_bev <- read_csv("01_RAW_DATA/Gov_BC_AlexBevington/hydrometric_locations_2023.csv")
  
  # Format sites
  bcgov_bev_sites <- bcgov_bev %>%
    rename(site_name = STATION_NAME,
           site_uid = STATION_ID) %>%
    mutate(dataset_unique_identifier = "SWP_DTS_A151") %>%
    select(-Z)
  
  bcgov_bev_sites_sf <- st_as_sf(bcgov_bev_sites, crs = 4326, coords = c("LON", "LAT"))
  

# Prepare DFO Somass monitoring sites -------------------------------------
dfo_somass <- read_sf("02_PROCESSED_DATA/DFO_HowardStiff/Somass_Tw_Locations_DFO.shp")
  
  # Format sites
  dfo_somass_sites <- dfo_somass %>%
    rename(site_name = Gauge_Name,
           site_uid = Gauge_ID) %>%
    mutate(dataset_unique_identifier = "SWP_DTS_A158")
  
  dfo_somass_sites_sf <- st_transform(dfo_somass_sites, crs = 4326)
  
# Prepare DFO Yukon monitoring sites -------------------------------------
# Sites are being reviewed/retrieved by team in Whitehorse
# This version only has the sites for which I have good confidence
# csv file was prepared based on package of files provided
dfo_yukon <- read_csv("02_PROCESSED_DATA/DFO_Whitehorse/DFO_YY_Tw_Locations.csv")
  
  # Format sites
  dfo_yukon_sites <- dfo_yukon %>%
    mutate(dataset_unique_identifier = "SWP_DTS_A152")
  
  dfo_yukon_sites_sf <- st_as_sf(dfo_yukon_sites, crs = 4326, coords = c("lon", "lat"), remove = T)
  

# Prepare Reynolds Lab monitoring sites -----------------------------------
sfu_reynolds <- read_csv("02_PROCESSED_DATA/Reynolds_Lab_SFU/ReynoldsLab_MonitoringSites.csv")
  
  # Format sites
  sfu_reynolds_sites <- sfu_reynolds %>%
    mutate(dataset_unique_identifier = "SWP_DTS_A156")
  
  sfu_reynolds_sites_sf <- st_as_sf(sfu_reynolds_sites, 
                                    crs = 32609, coords = c("easting", "northing"), 
                                    remove = T) %>%
    st_transform(crs = 4326)


# Prepare Elk River Alliance monitoring sites -----
# Those seven sites below can be merged if necessary, but they are separate entries in the catalog
# Connects to Columbia Basin Data Hub
cbdh <- ckanr_setup("https://data.cbwaterhub.ca/") # No need for a key here
  
  # Wilson Creek 1
  # Get the CKAN resource
  era_wil1 <- resource_show(id = "7316923c-8223-4d4e-8b4f-9b2a0a4575ba",  
                           as = "table")
  # Download the file into the current session
  fetch_era_wil1 <- ckan_fetch(era_wils$url, "session", format = ".csv")
  # Format file
  era_wil1_site <- fetch_era_wil1 %>%
    first() %>% # Only need the first record to get coordinates
    select(site_id, latitude, longitude) %>%
    mutate(site_uid = site_id) %>% # llc for Living Lakes Canada; nk for North Kootenay
    mutate(site_name = "Wilson Creek 1",
           dataset_unique_identifier = "SWP_DTS_A076")
  
  era_wil1_site_sf <- st_as_sf(era_wil1_site, crs = 4326, coords = c('longitude', 'latitude'))
  
  # Lizard Creek 6
  # Get the CKAN resource
  era_liz6 <- resource_show(id = "6a3eafbe-7245-48d6-99f9-f1c4ef84cc75", 
                            as = "table")
  # Download the file into the current session
  fetch_era_liz6 <- ckan_fetch(era_liz6$url, "session", format = ".csv")
  # Format file
  era_liz6_site <- fetch_era_liz6 %>%
    first() %>% # Only need the first record to get coordinates
    select(site_id, latitude, longitude) %>%
    mutate(site_uid = site_id) %>% # llc for Living Lakes Canada; nk for North Kootenay
    mutate(site_name = "Liza Creek 6",
           dataset_unique_identifier = "SWP_DTS_A077")
  
  era_liz6_site_sf <- st_as_sf(era_liz6_site, crs = 4326, coords = c('longitude', 'latitude'))
  
  # Lizard Creek 5
  # Get the CKAN resource
  era_liz5 <- resource_show(id = "91e6da27-7bb5-4e3b-a12e-6fc5c56fbe4d",  
                            as = "table")
  # Download the file into the current session
  fetch_era_liz5 <- ckan_fetch(era_liza$url, "session", format = ".csv")
  # Format file
  era_liz5_site <- fetch_era_liz5 %>%
    first() %>% # Only need the first record to get coordinates
    select(site_id, latitude, longitude) %>%
    mutate(site_uid = site_id) %>% # llc for Living Lakes Canada; nk for North Kootenay
    mutate(site_name = "Liza Creek 5",
           dataset_unique_identifier = "SWP_DTS_A078")
  
  era_liz5_site_sf <- st_as_sf(era_liz5_site, crs = 4326, coords = c('longitude', 'latitude'))
  
  # Elk River 1
  # Get the CKAN resource
  era_elk1 <- resource_show(id = "acb69a6e-4ac0-49af-bae8-c7165ca409d5",
                            as = "table")
  # Download the file into the current session
  fetch_era_elk1 <- ckan_fetch(era_elk1$url, "session", format = ".csv")
  # Format file
  era_elk1_site <- fetch_era_elk1 %>%
    first() %>% # Only need the first record to get coordinates
    select(site_id, latitude, longitude) %>%
    mutate(site_uid = site_id) %>% # llc for Living Lakes Canada; nk for North Kootenay
    mutate(site_name = "Elk River 1",
           dataset_unique_identifier = "SWP_DTS_A080")
  
  era_elk1_site_sf <- st_as_sf(era_elk1_site, crs = 4326, coords = c('longitude', 'latitude'))
  
  # Elk River 2
  # Get the CKAN resource
  era_elk2 <- resource_show(id = "71a7574e-cc62-40ea-b152-6b02789e6344", 
                            as = "table")
  # Download the file into the current session
  fetch_era_elk2 <- ckan_fetch(era_elk2$url, "session", format = ".csv")
  # Format file
  era_elk2_site <- fetch_era_elk2 %>%
    first() %>% # Only need the first record to get coordinates
    select(site_id, latitude, longitude) %>%
    mutate(site_uid = site_id) %>% # llc for Living Lakes Canada; nk for North Kootenay
    mutate(site_name = "Elk River 2",
           dataset_unique_identifier = "SWP_DTS_A085")
  
  era_elk2_site_sf <- st_as_sf(era_elk2_site, crs = 4326, coords = c('longitude', 'latitude'))
  
  # Michel Creek 3
  # Get the CKAN resource
  era_mic3 <- resource_show(id = "9e0a4c20-b49c-45e5-bed7-078d9980f848",
                            as = "table")
  # Download the file into the current session
  fetch_era_mic3 <- ckan_fetch(era_mic3$url, "session", format = ".csv")
  # Format file
  era_mic3_site <- fetch_era_mic3 %>%
    first() %>% # Only need the first record to get coordinates
    select(site_id, latitude, longitude) %>%
    mutate(site_uid = site_id) %>% # llc for Living Lakes Canada; nk for North Kootenay
    mutate(site_name = "Michel Creek 3",
           dataset_unique_identifier = "SWP_DTS_A082")
  
  era_mic3_site_sf <- st_as_sf(era_mic3_site, crs = 4326, coords = c('longitude', 'latitude'))
  
  # Michel Creek 2
  # Get the CKAN resource
  era_mic2 <- resource_show(id = "de4c18bf-e44a-4df7-aaf3-5bc06a16d244",
                            as = "table")
  # Download the file into the current session
  fetch_era_mic2 <- ckan_fetch(era_mic2$url, "session", format = ".csv")
  # Format file
  era_mic2_site <- fetch_era_mic2 %>%
    first() %>% # Only need the first record to get coordinates
    select(site_id, latitude, longitude) %>%
    mutate(site_uid = site_id) %>% # llc for Living Lakes Canada; nk for North Kootenay
    mutate(site_name = "Michel Creek 2",
           dataset_unique_identifier = "SWP_DTS_A083")
  
  era_mic2_site_sf <- st_as_sf(era_mic2_site, crs = 4326, coords = c('longitude', 'latitude'))
  
  # Michel Creek 1
  # Get the CKAN resource
  era_mic1 <- resource_show(id = "4a0c15d5-e971-4de6-bb2b-617cca7306df", 
                            as = "table")
  # Download the file into the current session
  fetch_era_mic1 <- ckan_fetch(era_mic1$url, "session", format = ".csv")
  # Format file
  era_mic1_site <- fetch_era_mic1 %>%
    first() %>% # Only need the first record to get coordinates
    select(site_id, latitude, longitude) %>%
    mutate(site_uid = site_id) %>% # llc for Living Lakes Canada; nk for North Kootenay
    mutate(site_name = "Michel Creek 1",
           dataset_unique_identifier = "SWP_DTS_A084")
  
  era_mic1_site_sf <- st_as_sf(era_mic1_site, crs = 4326, coords = c('longitude', 'latitude'))
  
  # Coal Creek 5
  # Get the CKAN resource
  era_col5 <- resource_show(id = "7b2d5404-70ce-4638-bd26-dcf3181fc376", 
                            as = "table")
  # Download the file into the current session
  fetch_era_col5 <- ckan_fetch(era_col5$url, "session", format = ".csv")
  # Format file
  era_col5_site <- fetch_era_col5 %>%
    first() %>% # Only need the first record to get coordinates
    select(site_id, latitude, longitude) %>%
    mutate(site_uid = site_id) %>% # llc for Living Lakes Canada; nk for North Kootenay
    mutate(site_name = "Coal Creek 5",
           dataset_unique_identifier = "SWP_DTS_A087")
  
  era_col5_site_sf <- st_as_sf(era_col5_site, crs = 4326, coords = c('longitude', 'latitude'))
  
  # Aldridge Creek 1
  # Get the CKAN resource
  era_ald1 <- resource_show(id = "1561ec43-cbc8-4fee-aadc-e1ca607fefe3",
                            as = "table")
  # Download the file into the current session
  fetch_era_ald1 <- ckan_fetch(era_ald1$url, "session", format = ".csv")
  # Format file
  era_ald1_site <- fetch_era_ald1 %>%
    first() %>% # Only need the first record to get coordinates
    select(site_id, latitude, longitude) %>%
    mutate(site_uid = site_id) %>% # llc for Living Lakes Canada; nk for North Kootenay
    mutate(site_name = "Aldridge Creek 1",
           dataset_unique_identifier = "SWP_DTS_A088")
  
  era_ald1_site_sf <- st_as_sf(era_ald1_site, crs = 4326, coords = c('longitude', 'latitude'))
  
  # Boivin Creek 2
  # Get the CKAN resource
  era_boi2 <- resource_show(id = "058b2daf-f577-4381-af24-3defa9a78c13", 
                            as = "table")
  # Download the file into the current session
  fetch_era_boi2 <- ckan_fetch(era_boi2$url, "session", format = ".csv")
  # Format file
  era_boi2_site <- fetch_era_boi2 %>%
    first() %>% # Only need the first record to get coordinates
    select(site_id, latitude, longitude) %>%
    mutate(site_uid = site_id) %>% # llc for Living Lakes Canada; nk for North Kootenay
    mutate(site_name = "Boivin Creek 2",
           dataset_unique_identifier = "SWP_DTS_A092")
  
  era_boi2_site_sf <- st_as_sf(era_boi2_site, crs = 4326, coords = c('longitude', 'latitude'))
  
  # Boivin Creek 1
  # Get the CKAN resource
  era_boi1 <- resource_show(id = "2fb37787-378c-4675-8af5-f8e4f7942e99", 
                            as = "table")
  # Download the file into the current session
  fetch_era_boi1 <- ckan_fetch(era_boi1$url, "session", format = ".csv")
  # Format file
  era_boi1_site <- fetch_era_boi1 %>%
    first() %>% # Only need the first record to get coordinates
    select(site_id, latitude, longitude) %>%
    mutate(site_uid = site_id) %>% # llc for Living Lakes Canada; nk for North Kootenay
    mutate(site_name = "Boivin Creek 1",
           dataset_unique_identifier = "SWP_DTS_A103")
  
  era_boi1_site_sf <- st_as_sf(era_boi1_site, crs = 4326, coords = c('longitude', 'latitude'))
  
  # Alexander Creek 1
  # Get the CKAN resource
  era_alx1 <- resource_show(id = "eb6808c6-67f3-4147-8900-3fa091879adc", 
                            as = "table")
  # Download the file into the current session
  fetch_era_alx1 <- ckan_fetch(era_alx1$url, "session", format = ".csv")
  # Format file
  era_alx1_site <- fetch_era_alx1 %>%
    first() %>% # Only need the first record to get coordinates
    select(site_id, latitude, longitude) %>%
    mutate(site_uid = site_id) %>% # llc for Living Lakes Canada; nk for North Kootenay
    mutate(site_name = "Alexander Creek 1",
           dataset_unique_identifier = "SWP_DTS_A111")
  
  era_alx1_site_sf <- st_as_sf(era_alx1_site, crs = 4326, coords = c('longitude', 'latitude'))
  
  # Coal Creek 3
  # Get the CKAN resource
  era_col3 <- resource_show(id = "f6ea8742-3d02-4828-b5c2-962ea239022d",
                            as = "table")
  # Download the file into the current session
  fetch_era_col3 <- ckan_fetch(era_col3$url, "session", format = ".csv")
  # Format file
  era_col3_site <- fetch_era_col3 %>%
    first() %>% # Only need the first record to get coordinates
    select(site_id, latitude, longitude) %>%
    mutate(site_uid = site_id) %>% # llc for Living Lakes Canada; nk for North Kootenay
    mutate(site_name = "Coal Creek 3",
           dataset_unique_identifier = "SWP_DTS_A114")
  
  era_col3_site_sf <- st_as_sf(era_col3_site, crs = 4326, coords = c('longitude', 'latitude'))
  
  # Morissey Creek 1
  # Get the CKAN resource
  era_mor1 <- resource_show(id = "f8eb6b0e-7bcd-4c20-a4ee-371671dcc447",
                            as = "table")
  # Download the file into the current session
  fetch_era_mor1 <- ckan_fetch(era_mor1$url, "session", format = ".csv")
  # Format file
  era_mor1_site <- fetch_era_mor1 %>%
    first() %>% # Only need the first record to get coordinates
    select(site_id, latitude, longitude) %>%
    mutate(site_uid = site_id) %>% # llc for Living Lakes Canada; nk for North Kootenay
    mutate(site_name = "Morrissey Creek 1",
           dataset_unique_identifier = "SWP_DTS_A119")
  
  era_mor1_site_sf <- st_as_sf(era_mor1_site, crs = 4326, coords = c('longitude', 'latitude'))
  
  # Lizard Creek 1
  # Get the CKAN resource
  era_liz1 <- resource_show(id = "ecf4e1ee-5bb2-4641-9c56-5ccadc98db7e",
                            as = "table")
  # Download the file into the current session
  fetch_era_liz1 <- ckan_fetch(era_liz1$url, "session", format = ".csv")
  # Format file
  era_liz1_site <- fetch_era_liz1 %>%
    first() %>% # Only need the first record to get coordinates
    select(site_id, latitude, longitude) %>%
    mutate(site_uid = site_id) %>% # llc for Living Lakes Canada; nk for North Kootenay
    mutate(site_name = "Lizard Creek 1",
           dataset_unique_identifier = "SWP_DTS_A121")
  
  era_liz1_site_sf <- st_as_sf(era_liz1_site, crs = 4326, coords = c('longitude', 'latitude'))
  
  # Merge all Elk River (Watershed) Alliance together
  era_all_sites_sf <- bind_rows(era_alx1_site_sf, era_liz1_site_sf, era_mor1_site_sf,
                                era_col3_site_sf, era_boi1_site_sf, era_boi2_site_sf,
                                era_ald1_site_sf, era_col5_site_sf, era_mic1_site_sf,
                                era_mic2_site_sf, era_mic3_site_sf, era_elk2_site_sf,
                                era_elk1_site_sf, era_liz5_site_sf, era_liz6_site_sf,
                                era_wil1_site_sf)
  
# Prepare Friends of Kootenay Lake monitoring sites ---------------------------
# Those sites below can be merged if necessary, but they are separate entries in the catalog
# Based on description, there are 10 sites, but the dataset only has one coordinate set for all 10 loggers...
# Needs some more investigation when actual data ingestion happens
# Connects to Columbia Basin Data Hub
cbdh <- ckanr_setup("https://data.cbwaterhub.ca/") # No need for a key here
  
  # Kootenay Lake (West arm)
  # Get the CKAN resource
  fkl_west <- resource_show(id = "17d6333b-aec9-45b3-9aea-dfeeca091c96",
                            as = "table")
  # Download the file into the current session
  fetch_fkl_west <- ckan_fetch(fkl_west$url, "session", format = ".csv")
  # Format file
  fkl_west_site <- fetch_fkl_west %>%
    first() %>% # Only need the first record to get coordinates
    select(site_id, latitude, longitude) %>%
    mutate(site_uid = site_id) %>% # llc for Living Lakes Canada; nk for North Kootenay
    mutate(site_name = "Kootenay Lake West Arm",
           dataset_unique_identifier = "SWP_DTS_A075")
  
  fkl_west_site_sf <- st_as_sf(fkl_west_site, crs = 4326, coords = c('longitude', 'latitude'))
  
  # Lake shore
  # Get the CKAN resource
  fkl_shor <- resource_show(id = "bf5e42a1-6578-4ab4-ada0-0f64a1636af7",
                            as = "table")
  # Download the file into the current session
  fetch_fkl_shor <- ckan_fetch(fkl_shor$url, "session", format = ".csv")
  # Format file
  fkl_shor_site <- fetch_fkl_shor %>%
    first() %>% # Only need the first record to get coordinates
    select(site_id, latitude, longitude) %>%
    mutate(site_uid = site_id) %>% # llc for Living Lakes Canada; nk for North Kootenay
    mutate(site_name = "Shore spawning Kokanee",
           dataset_unique_identifier = "SWP_DTS_A140")
  
  fkl_shor_site_sf <- st_as_sf(fkl_shor_site, crs = 4326, coords = c('longitude', 'latitude'))
  
  # Merge all Friends of Kootenay Lake together
  fkl_all_sites <- bind_rows(fkl_shor_site_sf, fkl_west_site_sf)
  
# # Prepare Nanwakolas Council monitoring sites -------------------------------------
# nanwakolas <- read_csv("01_RAW_DATA/Nanwakolas/NC50_draft_sites.csv")
#   
#   # Format sites
#   nanwakolas_sites <- nanwakolas %>%
#     mutate(site_uid = paste0("nwkls_", OID_),
#            site_name = `site_uid`,
#            lat = as.numeric(str_sub(DDLat, end = -2)),
#            lon = as.numeric(paste0("-", str_sub(DDLon, end = -2))),
#            dataset_unique_identifier = "SWP_DTS_A157") %>%
#     select(-(OID_))
#   
#   nanwakolas_sites_sf <- st_as_sf(nanwakolas_sites, crs = 4326, coords = c("lon", "lat"))
    

######################################
## 3) Create monitoring point layer ##
######################################
  
# Take all spatial layers created above and merge them
  
site_catalogue_sf <- bind_rows(bc_compiled_sf, hakai_sf,
                               unbc_sf, cosmo_sf, res_waters_sf, pss_sf, skt_ub_sf, 
                               pacfish_sf, swss_sheep_site_sf, swss_salmo_site_sf, 
                               swss_curtis_site_sf, swss_hidden_site_sf, swss_qua_site_sf, 
                               swss_clearwater_site_sf, swss_erie_site_sf, kitasoo_sites_sf, 
                               GWA_SFC_sites_sf, shushwap_sf, sfu_swl_sites_sf, 
                               ubc_moore_sites_sf, uofa_kluane_sites_sf, 
                               unbc_quesnel_sites_sf, scsk_sf, bcgov_bev_sites_sf, 
                               dfo_somass_sites_sf, dfo_yukon_sites_sf, sfu_reynolds_sites_sf,
                               llc_carlyle_site_sf, llc_gar_site_sf,
                               llc_mcdo_site_sf, llc_davis_site_sf, llc_bjerk_site_sf,
                               llc_koot_site_sf, llc_benh_site_sf, llc_ujl_site_sf, llc_sap_site_sf,
                               ross_hale_site_sf, ross_ceme_site_sf,
                               ross_cent_site_sf, ross_fala_site_sf, ross_golpher_site_sf,
                               ross_hale_site_sf, ross_milk_site_sf, ross_tiger_site_sf,
                               ross_topp_site_sf, ross_warf_site_sf, era_all_sites_sf,
                               fkl_all_sites) %>%
    left_join(catalog) %>% # This should automatically use the field "dataset_unique_identifier"
    select(-c(Id, comments, date_dts_pse, date_dts_catalog, data_sharing_agreement, data_acquired))


# Save catalogue layer
# Open format (mostly for use in QGIS)
st_write(site_catalogue_sf, dsn = "02_PROCESSED_DATA/PSF/catalogue_monitoring_sites.gpkg", delete_dsn = T) 
# ESRI FGDB format (for use in ArcGIS)
fgdb_path <- 

# End timer --------------------------------------------------------------
toc()
         