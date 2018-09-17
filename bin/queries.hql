CREATE DATABASE IF NOT EXISTS weather_db;
use weather_db;
CREATE EXTERNAL TABLE IF NOT EXISTS weather_details(stationNumber  int,year  int,month  int,day  int,hour  int,air_temperature  int,dew_temperature  int,sea_level_pressure  int,wind_direction  int,wind_speed_rate  int,sky_condition  int,liqd_per_dept_dimen_hr  int,liqd_per_dept_dimen_6hr  int) PARTITIONED BY (as_of_date string)
STORED AS PARQUET LOCATION '/user/ndulam/weather_refined_parq';
