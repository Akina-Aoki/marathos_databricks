CATALOG = "marathos_catalog"

BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"
DEFAULT_SCHEMA = "default"

RAW_VOLUME_BASE_DIR = f"/Volumes/{CATALOG}/{DEFAULT_SCHEMA}/raw"

RAW_MARATHON_RESULTS = f"{CATALOG}.{BRONZE_SCHEMA}.raw_marathon_results"
RAW_COUNTRY_CODES = f"{CATALOG}.{BRONZE_SCHEMA}.raw_country_codes"

MARATHON_RESULTS_OBT = f"{CATALOG}.{SILVER_SCHEMA}.marathon_results_obt"

DIM_EVENT = f"{CATALOG}.{GOLD_SCHEMA}.dim_event"
DIM_ATHLETE = f"{CATALOG}.{GOLD_SCHEMA}.dim_athlete"
DIM_COUNTRY = f"{CATALOG}.{GOLD_SCHEMA}.dim_country"
DIM_DATE = f"{CATALOG}.{GOLD_SCHEMA}.dim_date"
FACT_RESULTS = f"{CATALOG}.{GOLD_SCHEMA}.fact_results"

VW_DISTANCE_KM_RESULTS = f"{CATALOG}.{GOLD_SCHEMA}.vw_distance_km_results"
VW_DISTANCE_MI_RESULTS = f"{CATALOG}.{GOLD_SCHEMA}.vw_distance_mi_results"
VW_TIME_HOUR_RESULTS = f"{CATALOG}.{GOLD_SCHEMA}.vw_time_hour_results"
VW_GLOBAL_LEADERBOARD = f"{CATALOG}.{GOLD_SCHEMA}.vw_global_leaderboard"
VW_SEASONAL_RACE_TRENDS = f"{CATALOG}.{GOLD_SCHEMA}.vw_seasonal_race_trends"
VW_RUNNER_DEMOGRAPHICS = f"{CATALOG}.{GOLD_SCHEMA}.vw_runner_demographics"
VW_COUNTRY_SEASONAL_TRENDS = f"{CATALOG}.{GOLD_SCHEMA}.vw_country_seasonal_trends"