-- assuming we have a mapping table

-- create table odm.mapping
-- (
-- 	variable_name text not null,
-- 	variable_code integer,
--  site_code integer,
-- 	method_code integer,
-- 	source_code integer,
-- 	table_name text
-- )
-- ;
--
-- create unique index mapping_variable_name_uindex
-- 	on odm.mapping (variable_name)
-- ;

-- and a climdb table like:
--
-- create view weather.kbs002_011 as
-- SELECT 'KBS'::text AS "LTER_Site",
--     'LTERWS'::text AS "Station",
--     to_char((kbs002_006_cache.date)::timestamp with time zone, 'YYYYMMDD'::text) AS "Date",
--     kbs002_006_cache.precipitation AS "Daily_Precip_Total_mm",
--     kbs002_006_cache.flag_precip AS "Flag_Daily_Precip_Total_mm",
--     kbs002_006_cache.air_temp_mean AS "Daily_AirTemp_Mean_C",
--     kbs002_006_cache.flag_air_temp_mean AS "Flag_Daily_AirTemp_Mean_C",
--     kbs002_006_cache.air_temp_max AS "Daily_AirTemp_AbsMax_C",
--     kbs002_006_cache.flag_air_temp_max AS "Flag_Daily_AirTemp_AbsMax_C",
--     kbs002_006_cache.air_temp_min AS "Daily_AirTemp_AbsMin_C",
--     kbs002_006_cache.flag_air_temp_min AS "Flag_Daily_AirTemp_AbsMin_C",
--     kbs002_006_cache.air_pressure AS "Daily_AtmPressure_Mean_hpa",
--     kbs002_006_cache.flag_air_pressure AS "Flag_Daily_AtmPressure_Mean_hpa",
--     ((kbs002_006_cache.solar_radiation)::double precision * (0.0864)::double precision) AS "Daily_GlobalRad_Total_mjm2",
--     kbs002_006_cache.flag_solar_rad AS "Flag_Daily_GlobalRad_Total_mjm2",
--     kbs002_006_cache.rh AS "Daily_RH_Mean_pct",
--     kbs002_006_cache.flag_rh AS "Flag_Daily_RH_Mean_pct",
--     kbs002_006_cache.wind_direction_mean AS "Daily_WindDir_Mean_deg",
--     kbs002_006_cache.flag_wind_dir AS "Flag_Daily_WindDir_Mean_deg",
--     kbs002_006_cache.wind_speed_mean AS "Daily_WindSp_Mean_msec",
--     kbs002_006_cache.flag_wind_speed AS "Flag_Daily_WindSp_Mean_msec"
--    FROM weather.kbs002_006_cache;


-- In postgres we can create an odm data table with the following code.
--
-- TODO: Figure out how to force the time zone so I can extract the offset and get the utc datetime
-- currently it's all in utc

select
'' as Field,
  data_value,
  local_datetime,
  extract('timezone_hour' from local_datetime) as utc_offset,
  local_datetime at time zone 'UTC' as utc_datetime,
  site_code,
  variable_code,
  method_code,
  source_code,
  quality_control_level_code
from (
SELECT
  "Date"::timestamp with time zone as local_datetime,
  "Station",
     unnest(
      array [
      'Daily_Precip_Total_mm',
      'Daily_AirTemp_Mean_C',
      'Daily_AirTemp_AbsMax_C',
      'Daily_AirTemp_AbsMin_C'
      ]) AS variable,
  unnest(
      array [
      "Daily_Precip_Total_mm",
      "Daily_AirTemp_Mean_C",
      "Daily_AirTemp_AbsMax_C",
      "Daily_AirTemp_AbsMin_C"
      ]) AS data_value,
  unnest(
    array [
    "Flag_Daily_Precip_Total_mm",
    "Flag_Daily_AirTemp_Mean_C",
    "Flag_Daily_AirTemp_AbsMax_C",
    "Flag_Daily_AirTemp_AbsMin_C"
    ]) AS quality_control_level_code
FROM weather.kbs002_011) as t1
join odm.mapping on mapping.variable_name = t1.variable
