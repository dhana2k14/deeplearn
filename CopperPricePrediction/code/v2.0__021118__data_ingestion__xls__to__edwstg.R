#***********************************************************************************************************************
#           Extract Data from XLSX files and store them in SQL Database -- Production Code -- Copper Price Prediction
#***********************************************************************************************************************
# Initial Settings and Declarations
# Add R libraries from local directory
# Reading config file for Connection Strings

# .libPaths(c(.libPaths(), "./libraries"))
# xmlConfig <- xmlParse(file = "../config.xml")
# xmlConfig <- xmlToList(xmlConfig)
# connectionString <- paste0("DRIVER={SQL Server}; server=",xmlConfig$server_ip,"; database=",xmlConfig$db_name_input,";uid=",xmlConfig$username,"; pwd=",xmlConfig$password, sep="")
# conn <- odbcDriverConnect(connection = connectionString)
# file_path = xmlConfig$CPPxlspath

pkgs <- c("data.table", "lubridate", "RODBC", "RODBCext", "cellranger", "XML")
for (pkg in pkgs)
{
  if(!(pkg %in% rownames(installed.packages()))) { install.packages(pkg, repos = "https://cloud.r-project.org") }
}
invisible(suppressWarnings(suppressMessages(lapply(pkgs, require, character.only = TRUE))))

# xmlConfig <- xmlParse(file = "../config.xml")
# xmlConfig <- xmlToList(xmlConfig)
# connectionString <- paste0("DRIVER={SQL Server}; server=",xmlConfig$server_ip,"; database=",xmlConfig$db_name_input,";uid=",xmlConfig$username,"; pwd=",xmlConfig$password, sep="")
# conn <- odbcDriverConnect(connection = connectionString)
# file_path = xmlConfig$CPPxlspath

# using Config CSV
setwd("D:\\COEA2A_Projects\\CopperPricePrediction")
csvConfig <- read.csv("../config.csv")
connectionString <- paste0("DRIVER={SQL Server}; server=",csvConfig$server_ip,"; database=",csvConfig$db_name_input,";uid=",csvConfig$username,"; pwd=",csvConfig$password, sep="")
conn <- odbcDriverConnect(connection = connectionString)
file_path = csvConfig$CPPxlspath

# 1. Copper Spot Prices 
copper_df <- as.data.table(readxl::read_excel(paste0(file_path,'Copper_LME_MCK.xlsx'), sheet = "Copper_LME_Price_History", range = cell_limits(c(2, 1), c(NA, 2))))
setnames(copper_df, 'Spot', 'copper_price')
copper_df$Currency <- 'us dollar'
copper_df$Unit_Of_Measure <- 'tonne'
copper_df$Date <- as.POSIXct(copper_df$Date, format = '%Y-%m-%d')
copper_df <- copper_df[, c(1, 3, 4, 2)]
sqlExecute(conn, 'DELETE FROM COEA2A.CPP_Prices_Spot')
sqlExecute(conn, "INSERT INTO COEA2A.CPP_Prices_Spot VALUES (?,?,?,?)", copper_df)

# Copper Concentrate index on TC & RC
## 1. Treating index
tc_df <- as.data.table(readxl::read_excel(paste0(file_path, "Copper_Inputs_MCK.xlsx"), sheet = 'Copper_Input_prices', range = cell_cols("AK:AM")))
setnames(tc_df, "Metal Bulletin Copper Concentrates Index TC", 'date')
setnames(tc_df, 'X__2', 'copper_tc_index')
tc_df$X__1 <- NULL
tc_df <- tc_df[c(-1),]
tc_df$date <- as.POSIXct(tc_df$date, format = "%d.%m.%Y")
tc_df$copper_tc_index <- as.numeric(tc_df$copper_tc_index)

## 2. Refining index
rc_df <- as.data.table(readxl::read_excel(paste0(file_path, "Copper_Inputs_MCK.xlsx"), sheet = 'Copper_Input_prices', range = cell_cols("AV:AX")))
setnames(rc_df, "Metal Bulletin Copper Concentrates Index RC", 'date')
setnames(rc_df, 'X__2', 'copper_rc_index')
rc_df$X__1 <- NULL
rc_df <- rc_df[c(-1),]
rc_df$date <- as.POSIXct(rc_df$date, format = "%d.%m.%Y")
rc_df$copper_rc_index <- as.numeric(rc_df$copper_rc_index)

# Merging TC & RC Indices
concentrate_df <- merge(tc_df, rc_df, by = 'date', all.x = TRUE)
sqlExecute(conn, 'DELETE FROM COEA2A.CPP_Concentrate_Indices')
sqlExecute(conn, "INSERT INTO COEA2A.CPP_Concentrate_Indices VALUES (?,?,?)", concentrate_df)

# Copper Cathode
# 1. Free market data
fm_df <- as.data.table(readxl::read_excel(paste0(file_path, "Copper_Inputs_MCK.xlsx"), sheet = 'Copper_Input_prices', range = cell_cols("EQ:ES")))
names(fm_df) <- c('date', 'X__1', 'cu_cathode_fm')
fm_df$X__1 <- NULL 
fm_df <- fm_df[c(-1),]
fm_df$date <- as.POSIXct(fm_df$date, format = "%d.%m.%Y")
fm_df$cu_cathode_fm <- as.numeric(fm_df$cu_cathode_fm)
fm_df$cu_cathode_fm <- round(fm_df$cu_cathode_fm / 100, 5)
fm_df$cu_cathode_fm <- round(fm_df$cu_cathode_fm * 0.0005, 5)

# 2. Domestic US 
us_df <- as.data.table(readxl::read_excel(paste0(file_path, "Copper_Inputs_MCK.xlsx"), sheet = 'Copper_Input_prices', range = cell_cols("FB:FD")))
names(us_df) <- c('date', 'X__1', 'cu_cathode_us_dom')
us_df$X__1 <- NULL
us_df <- us_df[c(-1),]
us_df$date <- as.POSIXct(us_df$date, format = "%d.%m.%Y")
us_df$cu_cathode_us_dom <- as.numeric(us_df$cu_cathode_us_dom)
us_df$cu_cathode_us_dom <- round(us_df$cu_cathode_us_dom / 100, 5)
us_df$cu_cathode_us_dom <- round(us_df$cu_cathode_us_dom * 0.0005, 5)

# 3. South Africa LME
rsa_df <- as.data.table(readxl::read_excel(paste0(file_path, "Copper_Inputs_MCK.xlsx"), sheet = 'Copper_Input_prices', range = cell_cols("FM:FO")))
names(rsa_df) <- c('date', 'X__1', 'cu_cathode_rsa')
rsa_df$X__1 <- NULL
rsa_df <- rsa_df[c(-1),]
rsa_df$date <- as.POSIXct(rsa_df$date, format = "%d.%m.%Y")
rsa_df$cu_cathode_rsa <- as.numeric(rsa_df$cu_cathode_rsa)

# 4. Rotterdam 
rott_df <- as.data.table(readxl::read_excel(paste0(file_path, "Copper_Inputs_MCK.xlsx"), sheet = 'Copper_Input_prices', range = cell_cols("GT:GV")))
names(rott_df) <- c('date', 'X__1', 'cu_cathode_rotterdam')
rott_df$X__1 <- NULL
rott_df <- rott_df[c(-1),]
rott_df$date <- as.POSIXct(rott_df$date, format = "%d.%m.%Y")
rott_df$cu_cathode_rotterdam <- as.numeric(rott_df$cu_cathode_rotterdam)

# 5. Germany
ger_df <- as.data.table(readxl::read_excel(paste0(file_path, "Copper_Inputs_MCK.xlsx"), sheet = 'Copper_Input_prices', range = cell_cols("HE:HG")))
names(ger_df) <- c('date', 'X__1', 'cu_cathode_germany')
ger_df$X__1 <- NULL
ger_df <- ger_df[c(-1),]
ger_df$date <- as.POSIXct(ger_df$date, format = "%d.%m.%Y")
ger_df$cu_cathode_germany <- as.numeric(ger_df$cu_cathode_germany)

# 6. Shangai
sha_df <- as.data.table(readxl::read_excel(paste0(file_path, "Copper_Inputs_MCK.xlsx"), sheet = 'Copper_Input_prices', range = cell_cols("IA:IC"))) 
names(sha_df) <- c('date', 'X__1', 'cu_cathode_shangai')
sha_df$X__1 <- NULL
sha_df <- sha_df[c(-1),]
sha_df$date <- as.POSIXct(sha_df$date, format = "%d.%m.%Y")
sha_df$cu_cathode_shangai <- as.numeric(sha_df$cu_cathode_shangai)

# 7. Japan
jap_df <- as.data.table(readxl::read_excel(paste0(file_path, "Copper_Inputs_MCK.xlsx"), sheet = 'Copper_Input_prices', range = cell_cols("IL:IN"))) 
names(jap_df) <- c('date', 'X__1', 'cu_cathode_japan')
jap_df$X__1 <- NULL
jap_df <- jap_df[c(-1),]
jap_df$date <- as.POSIXct(jap_df$date, format = "%d.%m.%Y")
jap_df$cu_cathode_japan <- as.numeric(jap_df$cu_cathode_japan)

# Taiwan
tai_df <- as.data.table(readxl::read_excel(paste0(file_path, "Copper_Inputs_MCK.xlsx"), sheet = 'Copper_Input_prices', range = cell_cols("IW:IY"))) 
names(tai_df) <- c('date', 'X__1', 'cu_cathode_taiwan')
tai_df$X__1 <- NULL
tai_df <- tai_df[c(-1),]
tai_df$date <- as.POSIXct(tai_df$date, format = "%d.%m.%Y")
tai_df$cu_cathode_taiwan <- as.numeric(tai_df$cu_cathode_taiwan)

# Southeast Asia
sa_df <- as.data.table(readxl::read_excel(paste0(file_path, "Copper_Inputs_MCK.xlsx"), sheet = 'Copper_Input_prices', range = cell_cols("JS:JU")))  
names(sa_df) <- c('date', 'X__1', 'cu_cathode_seasia')
sa_df$X__1 <- NULL
sa_df <- sa_df[c(-1),]
sa_df$date <- as.POSIXct(sa_df$date, format = '%d.%m.%Y')
sa_df$cu_cathode_seasia <- as.numeric(sa_df$cu_cathode_seasia)

# Merging 
us_df <- merge(us_df, fm_df, by = 'date', all.x = T)
us_df <- merge(us_df, rsa_df, by = 'date', all.x = T)
us_df <- merge(us_df, ger_df, by = 'date', all.x = T)
us_df <- merge(us_df, jap_df, by = 'date', all.x = T)
us_df <- merge(us_df, rott_df, by = 'date', all.x = T)
us_df <- merge(us_df, sa_df, by = 'date', all.x = T)
us_df <- merge(us_df, sha_df, by = 'date', all.x = T)
us_df <- merge(us_df, tai_df, by = 'date', all.x = T)

# add currency and unit_of_measure
us_df$currency <- 'us dollar'
us_df$unit_of_measure <- 'tonne'

# re-arrange columns as per sql table
us_df <- us_df[,c(1, 3, 2, 4, 7, 5, 9, 6, 10, 8, 11, 12)]
sqlExecute(conn, 'DELETE FROM COEA2A.CPP_Cathode_Prices')
sqlExecute(conn, "INSERT INTO COEA2A.CPP_Cathode_Prices VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", us_df)

# Crude Oil Indices
co_df <- as.data.table(readxl::read_excel(paste0(file_path, "Copper_Crude_Oil_Index.xlsx"), sheet = 'Crude_Oil_WTI_Futures_History', range = cell_cols("A:B"))) 
names(co_df) <- c('date', 'crude_oil_index')
co_df$date <- as.POSIXct(co_df$date, format = "%Y-%m-%d")
co_df$crude_oil_index <- as.numeric(co_df$crude_oil_index)
sqlExecute(conn, 'DELETE FROM COEA2A.CPP_Crude_Oil_Indices')
sqlExecute(conn, "INSERT INTO COEA2A.CPP_Crude_Oil_Indices VALUES (?,?)", co_df)

# Supply and Demand 
temp_df <- as.data.table(readxl::read_excel(paste0(file_path, "Copper_LME_MCK.xlsx"), sheet = 'Copper_supply_demand_balance', range = cell_cols("Y:DH"))) 
temp_df <- temp_df[c(7, 14),]
colnames(temp_df) <- as.character(seq(as.Date('1998-01-01'), as.Date('2019-12-01'), by = 'quarter'))
temp_df <- as.data.frame(t(temp_df))
setnames(temp_df, 'V1', 'Production')
setnames(temp_df, 'V2', 'Consumption')
temp_df$date <- rownames(temp_df)
rownames(temp_df) <- NULL
temp_df$Production <- round(as.numeric(as.character(temp_df$Production)), 0)
temp_df$Consumption <- round(as.numeric(as.character(temp_df$Consumption)), 0)
temp_df$date <- as.POSIXct(temp_df$date, format = "%Y-%m-%d")
temp_df <- temp_df[,c(3, 1, 2)]
sqlExecute(conn, "DELETE FROM COEA2A.CPP_Demand_Supply")
sqlExecute(conn, "INSERT INTO COEA2A.CPP_Demand_Supply VALUES (?,?,?)", temp_df)

# Copper Scrap wires 
## 1. NY1 heavy copper & wire - 1
heavy_wire_1_df <- as.data.table(readxl::read_excel(paste0(file_path, "Copper_Inputs_MCK.xlsx"), sheet = 'Copper_Input_prices', range = cell_cols("Z:AB"))) 
names(heavy_wire_1_df) <- c('date', 'X__1', 'heavy_cu_wire_1')
heavy_wire_1_df$X__1 <- NULL
heavy_wire_1_df <- heavy_wire_1_df[c(-1),]
heavy_wire_1_df$date <- as.POSIXct(heavy_wire_1_df$date, format = "%d.%m.%Y")
heavy_wire_1_df$heavy_cu_wire_1 <- as.numeric(heavy_wire_1_df$heavy_cu_wire_1)
heavy_wire_1_df$heavy_cu_wire_1 <- round(heavy_wire_1_df$heavy_cu_wire_1 / 100, 5)
heavy_wire_1_df$heavy_cu_wire_1 <- round(heavy_wire_1_df$heavy_cu_wire_1 * 0.0005, 5)

## 2. NY1 heavy copper & wire - 2
heavy_wire_2_df <- as.data.table(readxl::read_excel(paste0(file_path, "Copper_Inputs_MCK.xlsx"), sheet = 'Copper_Input_prices', range = cell_cols("O:Q"))) 
names(heavy_wire_2_df) <- c('date', 'X__1', 'heavy_cu_wire_2')
heavy_wire_2_df$X__1 <- NULL
heavy_wire_2_df <- heavy_wire_2_df[c(-1),]
heavy_wire_2_df$date <- as.POSIXct(heavy_wire_2_df$date, format = "%d.%m.%Y")
heavy_wire_2_df$heavy_cu_wire_2 <- as.numeric(heavy_wire_2_df$heavy_cu_wire_2)
heavy_wire_2_df$heavy_cu_wire_2 <- round(heavy_wire_2_df$heavy_cu_wire_2 / 100, 5)
heavy_wire_2_df$heavy_cu_wire_2 <- round(heavy_wire_2_df$heavy_cu_wire_2 * 0.0005, 5)

## 3. Red brass scrap 
red_df <- as.data.table(readxl::read_excel(paste0(file_path, "Copper_Inputs_MCK.xlsx"), sheet = 'Copper_Input_prices', range = cell_cols("CC:CE"))) 
names(red_df) <- c('date', 'X__1', 'redbrass_scrap')
red_df$X__1 <- NULL
red_df <- red_df[c(-1),]
red_df$date <- as.POSIXct(red_df$date, format = "%d.%m.%Y")
red_df$redbrass_scrap <- as.numeric(red_df$redbrass_scrap)
red_df$redbrass_scrap <- round(red_df$redbrass_scrap / 100, 5)
red_df$redbrass_scrap <- round(red_df$redbrass_scrap * 0.0005, 5)

## 4. Yellow brass scrap
yellow_df <- as.data.table(readxl::read_excel(paste0(file_path, "Copper_Inputs_MCK.xlsx"), sheet = 'Copper_Input_prices', range = cell_cols("CN:CP"))) 
names(yellow_df) <- c('date', 'X__1', 'yellowbrass_scrap')
yellow_df$X__1 <- NULL
yellow_df <- yellow_df[c(-1),]
yellow_df$date <- as.POSIXct(yellow_df$date, format = "%d.%m.%Y")
yellow_df$yellowbrass_scrap <- as.numeric(yellow_df$yellowbrass_scrap)
yellow_df$yellowbrass_scrap <- round(yellow_df$yellowbrass_scrap / 100, 5)
yellow_df$yellowbrass_scrap <- round(yellow_df$yellowbrass_scrap * 0.0005, 5)

## 5. Wire Berry
berry_df <- as.data.table(readxl::read_excel(paste0(file_path, "Copper_Inputs_MCK.xlsx"), sheet = 'Copper_Input_prices', range = cell_cols("DJ:DL"))) 
names(berry_df) <- c('date', 'X__1', 'cu_wire_berry')
berry_df$X__1 <- NULL
berry_df <- berry_df[c(-1),]
berry_df$date <- as.POSIXct(berry_df$date, format = "%d.%m.%Y")
berry_df$cu_wire_berry <- as.numeric(berry_df$cu_wire_berry)
berry_df$cu_wire_berry <- round(berry_df$cu_wire_berry / 100, 5)
berry_df$cu_wire_berry <- round(berry_df$cu_wire_berry * 0.0005, 5)

## 6. Wire Barley 
barley_df <- as.data.table(readxl::read_excel(paste0(file_path, "Copper_Inputs_MCK.xlsx"), sheet = 'Copper_Input_prices', range = cell_cols("CY:DA"))) 
names(barley_df) <- c('date', 'X__1', 'cu_wire_barley')
barley_df$X__1 <- NULL
barley_df <- barley_df[c(-1),]
barley_df$date <- as.POSIXct(barley_df$date, format = "%d.%m.%Y")
barley_df$cu_wire_barley <- as.numeric(barley_df$cu_wire_barley)
barley_df$cu_wire_barley <- round(barley_df$cu_wire_barley / 100, 5)
barley_df$cu_wire_barley <- round(barley_df$cu_wire_barley * 0.0005, 5)

## 7. Heavy Copper Scrap
heavy_cp_scrap <- as.data.table(readxl::read_excel(paste0(file_path, "Copper_Inputs_MCK.xlsx"), sheet = 'Copper_Input_prices', range = cell_cols("DU:DW"))) 
names(heavy_cp_scrap) <- c('date', 'X__1', 'heavy_cu_scrap')
heavy_cp_scrap$X__1 <- NULL
heavy_cp_scrap <- heavy_cp_scrap[c(-1),]
heavy_cp_scrap$date <- as.POSIXct(heavy_cp_scrap$date, format = "%d.%m.%Y")
heavy_cp_scrap$heavy_cu_scrap <- as.numeric(heavy_cp_scrap$heavy_cu_scrap)
heavy_cp_scrap$heavy_cu_scrap <- round(heavy_cp_scrap$heavy_cu_scrap / 100, 5)
heavy_cp_scrap$heavy_cu_scrap <- round(heavy_cp_scrap$heavy_cu_scrap * 0.0005, 5)

## 8. Copper wire Scrap 
cp_wire_scrap <- as.data.table(readxl::read_excel(paste0(file_path, "Copper_Inputs_MCK.xlsx"), sheet = 'Copper_Input_prices', range = cell_cols("EF:EH"))) 
names(cp_wire_scrap) <- c('date', 'X__1', 'cu_wire_scrap')
cp_wire_scrap$X__1 <- NULL
cp_wire_scrap <- cp_wire_scrap[c(-1),]
cp_wire_scrap$date <- as.POSIXct(cp_wire_scrap$date, format = "%d.%m.%Y")
cp_wire_scrap$cu_wire_scrap <- as.numeric(cp_wire_scrap$cu_wire_scrap)
cp_wire_scrap$cu_wire_scrap <- round(cp_wire_scrap$cu_wire_scrap / 100, 5)
cp_wire_scrap$cu_wire_scrap <- round(cp_wire_scrap$cu_wire_scrap * 0.0005, 5)

# Merging
heavy_wire_2_df <- merge(heavy_wire_2_df, barley_df, by = 'date', all.x = T)
heavy_wire_2_df <- merge(heavy_wire_2_df, berry_df, by = 'date', all.x = T)
heavy_wire_2_df <- merge(heavy_wire_2_df, heavy_wire_1_df, by = 'date', all.x = T)
heavy_wire_2_df <- merge(heavy_wire_2_df, cp_wire_scrap, by = 'date', all.x = T)
heavy_wire_2_df <- merge(heavy_wire_2_df, heavy_cp_scrap, by = 'date', all.x = T)
heavy_wire_2_df <- merge(heavy_wire_2_df, red_df, by = 'date', all.x = T)
heavy_wire_2_df <- merge(heavy_wire_2_df, yellow_df, by = 'date', all.x = T)
heavy_wire_2_df$currency <- 'us dollar'
heavy_wire_2_df$unit_of_measure <- 'tonne'

# re-arrange columns
heavy_wire_2_df <- heavy_wire_2_df[,c(1, 5, 2, 8, 9, 3, 4, 7, 6, 10, 11)]
sqlExecute(conn, "DELETE FROM COEA2A.CPP_Scrap_Prices" )
sqlExecute(conn, "INSERT INTO COEA2A.CPP_Scrap_Prices VALUES (?,?,?,?,?,?,?,?,?,?,?)", heavy_wire_2_df)
close(conn)







