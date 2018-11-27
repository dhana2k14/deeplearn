# *******************************************************************************************************************
#               Copper Price Forecast v2.0 - 02 November 2018 -- Production Code -- Future prices prediction
# *******************************************************************************************************************
# Initial Settings and Declarations
# Add R libraries from local directory
# Reading config file for Connection Strings

set.seed(424)
options(scipen = 999)
output <- data.frame()
number_of_weeks <- 16
# .libPaths(c(.libPaths(), "./libraries/"))
pkgs <- c("data.table", "lubridate", "RODBC", "RODBCext", "cellranger", "XML", "forecast", "tseries", "zoo")
for (pkg in pkgs)
{
  if(!(pkg %in% rownames(installed.packages()))) { install.packages(pkg, repos = "https://cloud.r-project.org") }
}
invisible(suppressWarnings(suppressMessages(lapply(pkgs, require, character.only = TRUE))))
# xmlConfig <- xmlParse(file = "../config.xml")
# xmlConfig <- xmlToList(xmlConfig)
# connectionString <- paste0("DRIVER={SQL Server}; server=",xmlConfig$server_ip,"; database=",xmlConfig$db_name_input,";uid=",xmlConfig$username,"; pwd=",xmlConfig$password, sep="")
# conn <- odbcDriverConnect(connection = connectionString)

# using Config CSV
setwd("D:\\COEA2A_Projects\\CopperPricePrediction")
csvConfig <- read.csv("../config.csv")
connectionString <- paste0("DRIVER={SQL Server}; server=",csvConfig$server_ip,"; database=",csvConfig$db_name_input,";uid=",csvConfig$username,"; pwd=",csvConfig$password, sep="")
conn <- odbcDriverConnect(connection = connectionString)
copper_df <- sqlQuery(conn, 'select * from COEA2A.CPP_Prices_Spot', stringsAsFactors = FALSE)
cu_cathode_df <- sqlQuery(conn, 'select * from COEA2A.CPP_Cathode_Prices', stringsAsFactors = FALSE)
cu_scrap_df <- sqlQuery(conn, 'select * from COEA2A.CPP_Scrap_Prices', stringsAsFactors = FALSE)
crude_oil_df <- sqlQuery(conn, 'select * from COEA2A.CPP_Crude_Oil_Indices', stringsAsFactors = FALSE)
cu_demand_df <- sqlQuery(conn, 'select * from COEA2A.CPP_Demand_Supply', stringsAsFactors = FALSE)
cu_concentrate_df <- sqlQuery(conn, 'select * from COEA2A.CPP_Concentrate_Indices', stringsAsFactors = FALSE)
close(conn)

# Timeseries model
for (weeks_lag in 2:number_of_weeks){
  
  # set the maximum date 
  max_date <- as.Date(max(copper_df$Date)) + weeks(weeks_lag)
  
  # Copper Spot Price table
  main_df <- as.data.table(subset(copper_df, Date >= '2013-12-31'))
  main_df$Date <- as.character(floor_date(as.POSIXct(main_df$Date, format = "%Y-%m-%d"), 'week'))
  main_df <- main_df[, lapply(.SD, mean, na.rm = T), .SDcols = 'copper_price', by = c("Date")]
  main_df$target <- main_df$copper_price
  temp_vars <- names(main_df)[!names(main_df) %in% c('Date', 'target', 'Currency', 'Unit_of_Measure')]
  main_df[, (temp_vars):=lapply(.SD, lag, n = weeks_lag), .SDcols = temp_vars]

  ## Copper Cathode Price table
  # Free market data
  temp_df <- as.data.table(subset(cu_cathode_df, select = c('Date', 'CU_Cathode_FM')))
  temp_df$Date <- as.character(floor_date(temp_df$Date, 'week'))
  temp_df$Date <- as.character(as.POSIXct(temp_df$Date, format = "%Y-%m-%d") + weeks(weeks_lag))
  temp_df <- subset(temp_df, Date >= '2013-12-31')
  cathode_free_market <- temp_df[, lapply(.SD, mean, na.rm = T), .SDcols = c('CU_Cathode_FM'), by = c('Date')]
  
  # US Domestic data 
  temp_df <- as.data.table(subset(cu_cathode_df, select = c('Date', 'CU_Cathode_US_DOM')))
  temp_df$Date <- as.character(floor_date(temp_df$Date, 'week'))
  temp_df$Date <- as.character(as.POSIXct(temp_df$Date, format = "%Y-%m-%d") + weeks(weeks_lag))
  temp_df <- subset(temp_df, Date >= '2013-12-31')
  cathode_us_domestic <- temp_df[, lapply(.SD, mean, na.rm = T), .SDcols = c('CU_Cathode_US_DOM'), by = c('Date')]
  
  # Cathode - Rotterdam
  temp_df <- as.data.table(subset(cu_cathode_df, select = c('Date', 'CU_Cathode_Rotterdam')))
  temp_df$Date <- as.character(floor_date(temp_df$Date, 'week'))
  temp_df$Date <- as.character(as.POSIXct(temp_df$Date, format = "%Y-%m-%d") + weeks(weeks_lag))
  temp_df <- subset(temp_df, Date >= '2013-12-31')
  cathode_rotterdam <- temp_df[, lapply(.SD, mean, na.rm = T), .SDcols = c('CU_Cathode_Rotterdam'), by = c('Date')]
  
  # Cathode - Germany
  temp_df <- as.data.table(subset(cu_cathode_df, select = c('Date', 'CU_Cathode_Germany')))
  temp_df$Date <- as.character(floor_date(temp_df$Date, 'week'))
  temp_df$Date <- as.character(as.POSIXct(temp_df$Date, format = "%Y-%m-%d") + weeks(weeks_lag))
  temp_df <- subset(temp_df, Date >= '2013-12-31')
  cathode_germany <- temp_df[, lapply(.SD, mean, na.rm = T), .SDcols = c('CU_Cathode_Germany'), by = c('Date')]
  
  # Cathode - Shangai
  temp_df <- as.data.table(subset(cu_cathode_df, select = c('Date', 'CU_Cathode_Shangai')))
  temp_df$Date <- as.character(floor_date(temp_df$Date, 'week'))
  temp_df$Date <- as.character(as.POSIXct(temp_df$Date, format = "%Y-%m-%d") + weeks(weeks_lag))
  temp_df <- subset(temp_df, Date >= '2013-12-31')
  cathode_shangai <- temp_df[, lapply(.SD, mean, na.rm = T), .SDcols = c('CU_Cathode_Shangai'), by = c('Date')]
  
  # Cathode - Japan
  temp_df <- as.data.table(subset(cu_cathode_df, select = c('Date', 'CU_Cathode_Japan')))
  temp_df$Date <- as.character(floor_date(temp_df$Date, 'week'))
  temp_df$Date <- as.character(as.POSIXct(temp_df$Date, format = "%Y-%m-%d") + weeks(weeks_lag))
  temp_df <- subset(temp_df, Date >= '2013-12-31')
  cathode_japan <- temp_df[, lapply(.SD, mean, na.rm = T), .SDcols = c('CU_Cathode_Japan'), by = c('Date')]
  
  # Cathode - Taiwan
  temp_df <- as.data.table(subset(cu_cathode_df, select = c('Date', 'CU_Cathode_Taiwan')))
  temp_df$Date <- as.character(floor_date(temp_df$Date, 'week'))
  temp_df$Date <- as.character(as.POSIXct(temp_df$Date, format = "%Y-%m-%d") + weeks(weeks_lag))
  temp_df <- subset(temp_df, Date >= '2013-12-31')
  cathode_taiwan <- temp_df[, lapply(.SD, mean, na.rm = T), .SDcols = c('CU_Cathode_Taiwan'), by = c('Date')]
  
  # Cathode - Southeast asia
  temp_df <- as.data.table(subset(cu_cathode_df, select = c('Date', 'CU_Cathode_Seasia')))
  temp_df$Date <- as.character(floor_date(temp_df$Date, 'week'))
  temp_df$Date <- as.character(as.POSIXct(temp_df$Date, format = "%Y-%m-%d") + weeks(weeks_lag))
  temp_df <- subset(temp_df, Date >= '2013-12-31')
  cathode_seasia <- temp_df[, lapply(.SD, mean, na.rm = T), .SDcols = c('CU_Cathode_Seasia'), by = c('Date')]
  
  # Merging
  cathode_free_market <- merge(cathode_free_market, cathode_us_domestic, by = 'Date', all.x = T)
  cathode_free_market <- merge(cathode_free_market, cathode_rotterdam, by = 'Date', all.x = T)
  cathode_free_market <- merge(cathode_free_market, cathode_germany, by = 'Date', all.x = T)
  cathode_free_market <- merge(cathode_free_market, cathode_shangai, by = 'Date', all.x = T)
  cathode_free_market <- merge(cathode_free_market, cathode_japan, by = 'Date', all.x = T)
  cathode_free_market <- merge(cathode_free_market, cathode_taiwan, by = 'Date', all.x = T)
  cathode_free_market <- merge(cathode_free_market, cathode_seasia, by = 'Date', all.x = T)
 
  # Copper Supply and Demand
  cu_demand_df <- as.data.table(cu_demand_df)
  cu_demand_df$Date <- as.character(cu_demand_df$Date)
  temp_df <- as.data.table(as.character(seq.Date(from = as.Date("1998-01-01"), to = as.Date("2019-12-31"), by = 'month')))
  setnames(temp_df, 'V1', 'Date')
  temp_df <- merge(temp_df, cu_demand_df, by = 'Date', all.x = T)
  temp_df <- subset(temp_df, Date >= '2010-12-31')
  temp_df$Date <- as.character(floor_date(as.POSIXct(temp_df$Date, format = "%Y-%m-%d"), 'week'))
  temp_df$Date <- as.character(as.POSIXct(temp_df$Date, format = "%Y-%m-%d") + weeks(weeks_lag))
  cu_demand_supply_df <- temp_df[, lapply(.SD, na.locf)]
  
  ## Copper Scrap 
  # Heavy Copper Wire - 1
  temp_df <- as.data.table(subset(cu_scrap_df, select = c('Date', 'Heavy_CU_Wire_1')))
  temp_df$Date <- as.character(floor_date(temp_df$Date, 'week'))
  temp_df$Date <- as.character(as.POSIXct(temp_df$Date, format = "%Y-%m-%d") + weeks(weeks_lag))
  temp_df <- subset(temp_df, Date >= '2013-12-31')
  heavy_wire_1 <- temp_df[, lapply(.SD, mean, na.rm = T), .SDcols = c('Heavy_CU_Wire_1'), by = c('Date')]
  
  # Heavy Copper Wire - 2
  temp_df <- as.data.table(subset(cu_scrap_df, select = c('Date', 'Heavy_CU_Wire_2')))
  temp_df$Date <- as.character(floor_date(temp_df$Date, 'week'))
  temp_df$Date <- as.character(as.POSIXct(temp_df$Date, format = "%Y-%m-%d") + weeks(weeks_lag))
  temp_df <- subset(temp_df, Date >= '2013-12-31')
  heavy_wire_2 <- temp_df[, lapply(.SD, mean, na.rm = T), .SDcols = c('Heavy_CU_Wire_2'), by = c('Date')]
  
  # Redbrass scrap
  temp_df <- as.data.table(subset(cu_scrap_df, select = c('Date', 'Redbrass_Scrap')))
  temp_df$Date <- as.character(floor_date(temp_df$Date, 'week'))
  temp_df$Date <- as.character(as.POSIXct(temp_df$Date, format = "%Y-%m-%d") + weeks(weeks_lag))
  temp_df <- subset(temp_df, Date >= '2013-12-31')
  redbrass_df <- temp_df[, lapply(.SD, mean, na.rm = T), .SDcols = c('Redbrass_Scrap'), by = c('Date')]
  
  # Yellowbrass scarp
  temp_df <- as.data.table(subset(cu_scrap_df, select = c('Date', 'Yellowbrass_Scrap')))
  temp_df$Date <- as.character(floor_date(temp_df$Date, 'week'))
  temp_df$Date <- as.character(as.POSIXct(temp_df$Date, format = "%Y-%m-%d") + weeks(weeks_lag))
  temp_df <- subset(temp_df, Date >= '2013-12-31')
  yellowbrass_df <- temp_df[, lapply(.SD, mean, na.rm = T), .SDcols = c('Yellowbrass_Scrap'), by = c('Date')]
  
  # Wire - Barley
  temp_df <- as.data.table(subset(cu_scrap_df, select = c('Date', 'Cu_Wire_Barley')))
  temp_df$Date <- as.character(floor_date(temp_df$Date, 'week'))
  temp_df$Date <- as.character(as.POSIXct(temp_df$Date, format = "%Y-%m-%d") + weeks(weeks_lag))
  temp_df <- subset(temp_df, Date >= '2013-12-31')
  barley_df <- temp_df[, lapply(.SD, mean, na.rm = T), .SDcols = c('Cu_Wire_Barley'), by = c('Date')]
  
  # Wire - Berry
  temp_df <- as.data.table(subset(cu_scrap_df, select = c('Date', 'Cu_Wire_Berry')))
  temp_df$Date <- as.character(floor_date(temp_df$Date, 'week'))
  temp_df$Date <- as.character(as.POSIXct(temp_df$Date, format = "%Y-%m-%d") + weeks(weeks_lag))
  temp_df <- subset(temp_df, Date >= '2013-12-31')
  berry_df <- temp_df[, lapply(.SD, mean, na.rm = T), .SDcols = c('Cu_Wire_Berry'), by = c('Date')]
  
  # Heavy CU Scrap
  temp_df <- as.data.table(subset(cu_scrap_df, select = c('Date', 'Heavy_CU_Scrap')))
  temp_df$Date <- as.character(floor_date(temp_df$Date, 'week'))
  temp_df$Date <- as.character(as.POSIXct(temp_df$Date, format = "%Y-%m-%d") + weeks(weeks_lag))
  temp_df <- subset(temp_df, Date >= '2013-12-31')
  heavy_scrap_df <- temp_df[, lapply(.SD, mean, na.rm = T), .SDcols = c('Heavy_CU_Scrap'), by = c('Date')]
  
  # Wire Scrap
  temp_df <- as.data.table(subset(cu_scrap_df, select = c('Date', 'Cu_Wire_Scrap')))
  temp_df$Date <- as.character(floor_date(temp_df$Date, 'week'))
  temp_df$Date <- as.character(as.POSIXct(temp_df$Date, format = "%Y-%m-%d") + weeks(weeks_lag))
  temp_df <- subset(temp_df, Date >= '2013-12-31')
  scrap_df <- temp_df[, lapply(.SD, mean, na.rm = T), .SDcols = c('Cu_Wire_Scrap'), by = c('Date')]
  
  # Merging
  heavy_wire_1 <- merge(heavy_wire_1, heavy_wire_2, by = 'Date', all.x = T)
  heavy_wire_1 <- merge(heavy_wire_1, redbrass_df, by = 'Date', all.x = T)
  heavy_wire_1 <- merge(heavy_wire_1, yellowbrass_df, by = 'Date', all.x = T)
  heavy_wire_1 <- merge(heavy_wire_1, barley_df, by = 'Date', all.x = T)
  heavy_wire_1 <- merge(heavy_wire_1, berry_df, by = 'Date', all.x = T)
  heavy_wire_1 <- merge(heavy_wire_1, heavy_scrap_df, by = 'Date', all.x = T)
  heavy_wire_1 <- merge(heavy_wire_1, scrap_df, by = 'Date', all.x = T)
  
  # Crude Oil Indices
  temp_df <- as.data.table(crude_oil_df)
  temp_df$Date <- as.character(crude_oil_df$Date)

  # convert to weekly data
  temp_df_1 <- data.table(Date = as.character(seq.Date(from = as.Date("2012-01-01"), to = max_date, by = 'week')))
  temp_df <- merge(temp_df_1, temp_df, by = 'Date', all.x = T)
  temp_df$Date <- as.character(floor_date(as.POSIXct(temp_df$Date, format = "%Y-%m-%d"), 'week'))
  temp_df$Date <- as.character(as.POSIXct(temp_df$Date, format = "%Y-%m-%d") + weeks(weeks_lag))
  crude_oil_df <- temp_df[, 'Crude_Oil_Index':= lapply(.SD, na.locf), .SDcols = 'Crude_Oil_Index']
  
  ## Copper Concentrates
  # TC index
  temp_df <- as.data.table(subset(cu_concentrate_df, select = c('Date', 'CU_Concentrate_TC')))
  temp_df$Date <- as.character(floor_date(temp_df$Date, 'week'))
  temp_df$Date <- as.character(as.POSIXct(temp_df$Date, format = "%Y-%m-%d") + weeks(weeks_lag))
  tc_df <- temp_df[, lapply(.SD, mean, na.rm = T), .SDcols = c('CU_Concentrate_TC'), by = c('Date')]
  
  # RC Index
  temp_df <- as.data.table(subset(cu_concentrate_df, select = c('Date', 'CU_Concentrate_RC')))
  temp_df$Date <- as.character(floor_date(temp_df$Date, 'week'))
  temp_df$Date <- as.character(as.POSIXct(temp_df$Date, format = "%Y-%m-%d") + weeks(weeks_lag))
  rc_df <- temp_df[, lapply(.SD, mean, na.rm = T), .SDcols = c('CU_Concentrate_RC'), by = c('Date')]
  
  # Merging
  tc_df <- merge(tc_df, rc_df, by = 'Date', all.x = T)
  
  # Merging 
  main_df <- merge(main_df, cathode_free_market, by = 'Date', all.x = T)
  main_df <- merge(main_df, cu_demand_supply_df, by = 'Date', all.x = T)
  main_df <- merge(main_df, heavy_wire_1, by = 'Date', all.x = T)
  main_df <- merge(main_df, crude_oil_df, by = 'Date', all.x = T)
  main_df <- merge(main_df, tc_df, by = 'Date', all.x = T)
  
  # Missing values treatment 
  main_df[is.na(main_df)] <- NA
  main_df <- main_df[!is.na(target)]

  main_df$CU_Cathode_Rotterdam <- main_df[, lapply(.SD, na.locf), .SDcols = 'CU_Cathode_Rotterdam']
  main_df$CU_Cathode_Japan <- main_df[, lapply(.SD, na.locf), .SDcols = 'CU_Cathode_Japan']
  main_df$CU_Cathode_Shangai <- main_df[, lapply(.SD, na.locf), .SDcols = 'CU_Cathode_Shangai']
  main_df$CU_Cathode_Taiwan <- main_df[, lapply(.SD, na.locf), .SDcols = 'CU_Cathode_Taiwan']
  main_df$Production <- main_df[, lapply(.SD, na.locf), .SDcols = 'Production']
  main_df$Consumption <- main_df[, lapply(.SD, na.locf), .SDcols = 'Consumption']

  # Variable Selection
  features <- names(main_df)[!names(main_df) %in% c('Date', 'target')]
  xreg_features <- features[!features %in% c('Heavy_CU_Wire_1'
                                              , 'Heavy_CU_Wire_2'
                                              , 'CU_Cathode_Seasia'
                                              , 'CU_Cathode_Germany'
                                              , 'CU_Cathode_FM'
                                              , 'CU_Cathode_US_DOM'
                                              , 'Redbrass_Scrap'
                                              , 'Yellowbrass_Scrap'
                                              , 'Heavy_CU_Scrap'
                                              , 'Cu_Wire_Scrap'
                                              , 'Cu_Wire_Barley'
                                              , 'Cu_Wire_Berry'
                                              , 'CU_Concentrate_TC'
                                              , 'CU_Concentrate_RC')]
  
  # Timeseries model
  target <- ts(main_df$target, start = c(2014, 1), frequency = 52)
  target <- diff(target)
  model <- arima(x = target, order = c(0, 0, 2), xreg = main_df[-1, xreg_features, with = F])
  
  #************************************************************************************************
  #                                           Forecast
  #************************************************************************************************
  
  # Copper Spot Price table
  main_df <- as.data.table(subset(copper_df, Date >= '2013-12-31'))
  main_df$Date <- as.character(floor_date(as.POSIXct(main_df$Date, format = "%Y-%m-%d"), 'week'))
  main_df <- main_df[, lapply(.SD, mean, na.rm = T), .SDcols = 'copper_price', by = c("Date")]
  main_df$target <- main_df$copper_price

  ## Copper Cathode Price table
  # Free market data
  temp_df <- as.data.table(subset(cu_cathode_df, select = c('Date', 'CU_Cathode_FM')))
  temp_df$Date <- as.character(floor_date(temp_df$Date, 'week'))
  temp_df <- subset(temp_df, Date >= '2013-12-31')
  cathode_free_market <- temp_df[, lapply(.SD, mean, na.rm = T), .SDcols = c('CU_Cathode_FM'), by = c('Date')]
  
  # US Domestic data 
  temp_df <- as.data.table(subset(cu_cathode_df, select = c('Date', 'CU_Cathode_US_DOM')))
  temp_df$Date <- as.character(floor_date(temp_df$Date, 'week'))
  temp_df <- subset(temp_df, Date >= '2013-12-31')
  cathode_us_domestic <- temp_df[, lapply(.SD, mean, na.rm = T), .SDcols = c('CU_Cathode_US_DOM'), by = c('Date')]
  
  # Cathode - Rotterdam
  temp_df <- as.data.table(subset(cu_cathode_df, select = c('Date', 'CU_Cathode_Rotterdam')))
  temp_df$Date <- as.character(floor_date(temp_df$Date, 'week'))
  temp_df <- subset(temp_df, Date >= '2013-12-31')
  cathode_rotterdam <- temp_df[, lapply(.SD, mean, na.rm = T), .SDcols = c('CU_Cathode_Rotterdam'), by = c('Date')]
  
  # Cathode - Germany
  temp_df <- as.data.table(subset(cu_cathode_df, select = c('Date', 'CU_Cathode_Germany')))
  temp_df$Date <- as.character(floor_date(temp_df$Date, 'week'))
  temp_df <- subset(temp_df, Date >= '2013-12-31')
  cathode_germany <- temp_df[, lapply(.SD, mean, na.rm = T), .SDcols = c('CU_Cathode_Germany'), by = c('Date')]
  
  # Cathode - Shangai
  temp_df <- as.data.table(subset(cu_cathode_df, select = c('Date', 'CU_Cathode_Shangai')))
  temp_df$Date <- as.character(floor_date(temp_df$Date, 'week'))
  temp_df <- subset(temp_df, Date >= '2013-12-31')
  cathode_shangai <- temp_df[, lapply(.SD, mean, na.rm = T), .SDcols = c('CU_Cathode_Shangai'), by = c('Date')]
  
  # Cathode - Japan
  temp_df <- as.data.table(subset(cu_cathode_df, select = c('Date', 'CU_Cathode_Japan')))
  temp_df$Date <- as.character(floor_date(temp_df$Date, 'week'))
  temp_df <- subset(temp_df, Date >= '2013-12-31')
  cathode_japan <- temp_df[, lapply(.SD, mean, na.rm = T), .SDcols = c('CU_Cathode_Japan'), by = c('Date')]
  
  # Cathode - Taiwan
  temp_df <- as.data.table(subset(cu_cathode_df, select = c('Date', 'CU_Cathode_Taiwan')))
  temp_df$Date <- as.character(floor_date(temp_df$Date, 'week'))
  temp_df <- subset(temp_df, Date >= '2013-12-31')
  cathode_taiwan <- temp_df[, lapply(.SD, mean, na.rm = T), .SDcols = c('CU_Cathode_Taiwan'), by = c('Date')]
  
  # Cathode - Southeast asia
  temp_df <- as.data.table(subset(cu_cathode_df, select = c('Date', 'CU_Cathode_Seasia')))
  temp_df$Date <- as.character(floor_date(temp_df$Date, 'week'))
  temp_df <- subset(temp_df, Date >= '2013-12-31')
  cathode_seasia <- temp_df[, lapply(.SD, mean, na.rm = T), .SDcols = c('CU_Cathode_Seasia'), by = c('Date')]
  
  # Merging
  
  cathode_free_market <- merge(cathode_free_market, cathode_us_domestic, by = 'Date', all.x = T)
  cathode_free_market <- merge(cathode_free_market, cathode_rotterdam, by = 'Date', all.x = T)
  cathode_free_market <- merge(cathode_free_market, cathode_germany, by = 'Date', all.x = T)
  cathode_free_market <- merge(cathode_free_market, cathode_shangai, by = 'Date', all.x = T)
  cathode_free_market <- merge(cathode_free_market, cathode_japan, by = 'Date', all.x = T)
  cathode_free_market <- merge(cathode_free_market, cathode_taiwan, by = 'Date', all.x = T)
  cathode_free_market <- merge(cathode_free_market, cathode_seasia, by = 'Date', all.x = T)
  
  # Copper Supply and Demand
  cu_demand_df <- as.data.table(cu_demand_df)
  cu_demand_df$Date <- as.character(cu_demand_df$Date)
  temp_df <- as.data.table(as.character(seq.Date(from = as.Date("1998-01-01"), to = as.Date("2019-12-31"), by = 'month')))
  setnames(temp_df, 'V1', 'Date')
  temp_df <- merge(temp_df, cu_demand_df, by = 'Date', all.x = T)
  temp_df <- subset(temp_df, Date >= '2010-12-31')
  temp_df$Date <- as.character(floor_date(as.POSIXct(temp_df$Date, format = "%Y-%m-%d"), 'week'))
  cu_demand_supply_df <- temp_df[, lapply(.SD, na.locf)]
  
  ## Copper Scrap 
  # Heavy Copper Wire - 1
  temp_df <- as.data.table(subset(cu_scrap_df, select = c('Date', 'Heavy_CU_Wire_1')))
  temp_df$Date <- as.character(floor_date(temp_df$Date, 'week'))
  temp_df <- subset(temp_df, Date >= '2013-12-31')
  heavy_wire_1 <- temp_df[, lapply(.SD, mean, na.rm = T), .SDcols = c('Heavy_CU_Wire_1'), by = c('Date')]
  
  # Heavy Copper Wire - 2
  temp_df <- as.data.table(subset(cu_scrap_df, select = c('Date', 'Heavy_CU_Wire_2')))
  temp_df$Date <- as.character(floor_date(temp_df$Date, 'week'))
  temp_df <- subset(temp_df, Date >= '2013-12-31')
  heavy_wire_2 <- temp_df[, lapply(.SD, mean, na.rm = T), .SDcols = c('Heavy_CU_Wire_2'), by = c('Date')]
  
  # Redbrass scrap
  temp_df <- as.data.table(subset(cu_scrap_df, select = c('Date', 'Redbrass_Scrap')))
  temp_df$Date <- as.character(floor_date(temp_df$Date, 'week'))
  temp_df <- subset(temp_df, Date >= '2013-12-31')
  redbrass_df <- temp_df[, lapply(.SD, mean, na.rm = T), .SDcols = c('Redbrass_Scrap'), by = c('Date')]
  
  # Yellowbrass scarp
  temp_df <- as.data.table(subset(cu_scrap_df, select = c('Date', 'Yellowbrass_Scrap')))
  temp_df$Date <- as.character(floor_date(temp_df$Date, 'week'))
  temp_df <- subset(temp_df, Date >= '2013-12-31')
  yellowbrass_df <- temp_df[, lapply(.SD, mean, na.rm = T), .SDcols = c('Yellowbrass_Scrap'), by = c('Date')]
  
  # Wire - Barley
  temp_df <- as.data.table(subset(cu_scrap_df, select = c('Date', 'Cu_Wire_Barley')))
  temp_df$Date <- as.character(floor_date(temp_df$Date, 'week'))
  temp_df <- subset(temp_df, Date >= '2013-12-31')
  barley_df <- temp_df[, lapply(.SD, mean, na.rm = T), .SDcols = c('Cu_Wire_Barley'), by = c('Date')]
  
  # Wire - Berry
  temp_df <- as.data.table(subset(cu_scrap_df, select = c('Date', 'Cu_Wire_Berry')))
  temp_df$Date <- as.character(floor_date(temp_df$Date, 'week'))
  temp_df <- subset(temp_df, Date >= '2013-12-31')
  berry_df <- temp_df[, lapply(.SD, mean, na.rm = T), .SDcols = c('Cu_Wire_Berry'), by = c('Date')]
  
  # Heavy CU Scrap
  temp_df <- as.data.table(subset(cu_scrap_df, select = c('Date', 'Heavy_CU_Scrap')))
  temp_df$Date <- as.character(floor_date(temp_df$Date, 'week'))
  temp_df <- subset(temp_df, Date >= '2013-12-31')
  heavy_scrap_df <- temp_df[, lapply(.SD, mean, na.rm = T), .SDcols = c('Heavy_CU_Scrap'), by = c('Date')]
  
  # Wire Scrap
  temp_df <- as.data.table(subset(cu_scrap_df, select = c('Date', 'Cu_Wire_Scrap')))
  temp_df$Date <- as.character(floor_date(temp_df$Date, 'week'))
  temp_df <- subset(temp_df, Date >= '2013-12-31')
  scrap_df <- temp_df[, lapply(.SD, mean, na.rm = T), .SDcols = c('Cu_Wire_Scrap'), by = c('Date')]
  
  # Merging
  heavy_wire_1 <- merge(heavy_wire_1, heavy_wire_2, by = 'Date', all.x = T)
  heavy_wire_1 <- merge(heavy_wire_1, redbrass_df, by = 'Date', all.x = T)
  heavy_wire_1 <- merge(heavy_wire_1, yellowbrass_df, by = 'Date', all.x = T)
  heavy_wire_1 <- merge(heavy_wire_1, barley_df, by = 'Date', all.x = T)
  heavy_wire_1 <- merge(heavy_wire_1, berry_df, by = 'Date', all.x = T)
  heavy_wire_1 <- merge(heavy_wire_1, heavy_scrap_df, by = 'Date', all.x = T)
  heavy_wire_1 <- merge(heavy_wire_1, scrap_df, by = 'Date', all.x = T)
  
  # Crude Oil Indices
  temp_df <- as.data.table(crude_oil_df)
  temp_df$Date <- as.character(crude_oil_df$Date)
  
  # convert to weekly data
  temp_df_1 <- data.table(Date = as.character(seq.Date(from = as.Date("2012-01-01"), to = max_date, by = 'week')))
  temp_df <- merge(temp_df_1, temp_df, by = 'Date', all.x = T)
  temp_df$Date <- as.character(floor_date(as.POSIXct(temp_df$Date, format = "%Y-%m-%d"), 'week'))
  crude_oil_df <- temp_df[, 'Crude_Oil_Index':= lapply(.SD, na.locf), .SDcols = 'Crude_Oil_Index']
  
  ## Copper Concentrates
  # TC index
  temp_df <- as.data.table(subset(cu_concentrate_df, select = c('Date', 'CU_Concentrate_TC')))
  temp_df$Date <- as.character(floor_date(temp_df$Date, 'week'))
  tc_df <- temp_df[, lapply(.SD, mean, na.rm = T), .SDcols = c('CU_Concentrate_TC'), by = c('Date')]
  
  # RC Index
  temp_df <- as.data.table(subset(cu_concentrate_df, select = c('Date', 'CU_Concentrate_RC')))
  temp_df$Date <- as.character(floor_date(temp_df$Date, 'week'))
  rc_df <- temp_df[, lapply(.SD, mean, na.rm = T), .SDcols = c('CU_Concentrate_RC'), by = c('Date')]
  
  # Merging
  tc_df <- merge(tc_df, rc_df, by = 'Date', all.x = T)
  
  # Merging 
  main_df <- merge(main_df, cathode_free_market, by = 'Date', all.x = T)
  main_df <- merge(main_df, cu_demand_supply_df, by = 'Date', all.x = T)
  main_df <- merge(main_df, heavy_wire_1, by = 'Date', all.x = T)
  main_df <- merge(main_df, crude_oil_df, by = 'Date', all.x = T)
  main_df <- merge(main_df, tc_df, by = 'Date', all.x = T)
  
  # Missing values treatment 
  main_df[is.na(main_df)] <- NA
  main_df <- main_df[!is.na(target)]
  
  main_df$CU_Cathode_Rotterdam <- main_df[, lapply(.SD, na.locf), .SDcols = 'CU_Cathode_Rotterdam']
  main_df$CU_Cathode_Japan <- main_df[, lapply(.SD, na.locf), .SDcols = 'CU_Cathode_Japan']
  main_df$CU_Cathode_Shangai <- main_df[, lapply(.SD, na.locf), .SDcols = 'CU_Cathode_Shangai']
  main_df$CU_Cathode_Taiwan <- main_df[, lapply(.SD, na.locf), .SDcols = 'CU_Cathode_Taiwan']
  main_df$Production <- main_df[, lapply(.SD, na.locf), .SDcols = 'Production']
  main_df$Consumption <- main_df[, lapply(.SD, na.locf), .SDcols = 'Consumption']
  main_df$Crude_Oil_Index <- main_df[, lapply(.SD, na.locf), .SDcols = 'Crude_Oil_Index']
  
  # Prepare the forecast data  
  main_df$Date <- as.Date(main_df$Date)
  main_df <- main_df[order(-Date)]
  forecast_df <- main_df[1:weeks_lag]
  forecast_df <- forecast_df[order(Date)]
  forecast_df$prediction_date <- forecast_df$Date + weeks(weeks_lag)
  
  pred <- forecast(model, h = nrow(forecast_df), xreg = forecast_df[,xreg_features, with = F])
  forecast_df <- cbind(forecast_df, data.frame(prediction = data.frame(pred)$Point.Forecast))
  forecast_df$point.forecast <- numeric(0)
  forecast_df[1]$point.forecast <- forecast_df[nrow(forecast_df),]$target
  forecast_df$point.forecast <- forecast_df$point.forecast + forecast_df$prediction
  forecast_df[2:nrow(forecast_df)]$point.forecast <- forecast_df[2:nrow(forecast_df)]$prediction
  forecast_df$point.forecast <- cumsum(forecast_df$point.forecast)

  output <- rbind(output, data.frame(weeks_lag = weeks_lag, forecast_df[1:weeks_lag, c('prediction_date', 'point.forecast')]))
  
  message(weeks_lag) 
}

# Weighted average 
output <- as.data.table(output)
output <- output[order(prediction_date,weeks_lag)]
output[, weightage:= 100/seq(1:.N), by = "prediction_date"]
output[, prediction:= weighted.mean(point.forecast, weightage), by = "prediction_date"]
output <- output[order(prediction_date,weeks_lag)]
output <- output[, .SD[1], by = "prediction_date"]
output$weeks_lag <- NULL
output$weightage <- NULL

# Monthly Average 
output$pred_month <- ymd(paste0(dates = substr(output$prediction_date, 1, 7), day = '-01'))
output[, pred_price:=mean(prediction), by = 'pred_month']
output <- output[, .SD[1], by = 'pred_month']
output <- subset(output, select = c("pred_month", "pred_price"))
output$historic_pred_price <- NA # add this column with null values for sql insert
output <- output[,c("pred_month", "pred_price")]

# Actual Market prices dataset
main_df <- subset(main_df, select = c("Date", "target", "CU_Cathode_Rotterdam", "CU_Cathode_Shangai", "CU_Cathode_Japan", "CU_Cathode_Taiwan", "Production", "Consumption", "Crude_Oil_Index"))
main_df$actual_month <- ymd(paste0(dates = substr(main_df$Date, 1, 7), day = '-01'))
main_df[, actual_price:=mean(target), by = 'actual_month']
main_df <- main_df[, .SD[1], by = 'actual_month']
main_df <- subset(main_df, select = c("actual_month", "CU_Cathode_Rotterdam", "CU_Cathode_Shangai", "CU_Cathode_Japan", "CU_Cathode_Taiwan", "Production", "Consumption", "Crude_Oil_Index", "actual_price"))
main_df <- main_df[order(-actual_month)]
main_df <- main_df[1:4][order(actual_month)] # retain the recent four months 
main_df$Load_Date <- Sys.Date()

# Carry forward the actual price of recent month as predicted price of that month 
# to prevent the gap between the lines in POWER-BI
main_1_df <- main_df[order(-actual_month)][1,c("actual_month", "actual_price")]

#******************************************************************************************************************
#                                       Write the output to the EDW tables
#******************************************************************************************************************
connectionString <- paste0("DRIVER={SQL Server}; server=",csvConfig$server_ip,"; database=",csvConfig$db_name_output,";uid=",csvConfig$username,"; pwd=",csvConfig$password,sep="")
conn <- odbcDriverConnect(connection = connectionString)
sqlExecute(conn, "DELETE FROM COEA2A.CPP_Market_Prices")
sqlExecute(conn, "DELETE FROM COEA2A.CPP_Price_Forecasts")
sqlExecute(conn, "INSERT INTO COEA2A.CPP_Market_Prices VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", main_df)
sqlExecute(conn, "INSERT INTO COEA2A.CPP_Price_Forecasts VALUES (?, ?)", main_1_df)
sqlExecute(conn, "INSERT INTO COEA2A.CPP_Price_Forecasts VALUES (?, ?)", output)
close(conn)











