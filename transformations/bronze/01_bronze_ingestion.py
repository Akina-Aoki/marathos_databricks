from pyspark import pipelines as dp


# ------------------------------------------------------------
# Base paths in Unity Catalog raw volume
# ------------------------------------------------------------

BASE_DIR = "/Volumes/marathos_catalog/default/raw"

MARATHON_DATA_PATH = f"{BASE_DIR}/data"
MARATHON_SAMPLE_FILE = f"{MARATHON_DATA_PATH}/TWO_CENTURIES_OF_UM_RACES.csv"

COUNTRY_CODES_PATH = f"{BASE_DIR}/country_iso"
COUNTRY_CODES_SAMPLE_FILE = f"{COUNTRY_CODES_PATH}/marathos_country_codes.csv"


# ------------------------------------------------------------
# Parse schemas from CSV files
# ------------------------------------------------------------
# Streaming reads need a predefined schema.
# Spark cannot infer schema continuously while streaming.

marathon_schema = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(MARATHON_SAMPLE_FILE)
    .schema
)

country_codes_schema = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(COUNTRY_CODES_SAMPLE_FILE)
    .schema
)


# ------------------------------------------------------------
# Bronze table 1: Raw marathon results
# ------------------------------------------------------------

@dp.table(
    name="marathos_catalog.bronze.raw_marathon_results",
    comment="Raw ultra-marathon race results ingested into the Bronze layer.",
    table_properties={
        "delta.columnMapping.mode": "name",
        "delta.minReaderVersion": "2",
        "delta.minWriterVersion": "5",
    },
)
def raw_marathon_results():
    return (
        spark.readStream.format("csv")
        .options(header="true", inferSchema="true")
        .schema(marathon_schema)
        .load(MARATHON_DATA_PATH)
    )


# ------------------------------------------------------------
# Bronze table 2: Raw country code mapping
# ------------------------------------------------------------

@dp.table(
    name="marathos_catalog.bronze.raw_country_codes",
    comment="Raw Marathos country code mapping ingested into the Bronze layer.",
    table_properties={
        "delta.columnMapping.mode": "name",
        "delta.minReaderVersion": "2",
        "delta.minWriterVersion": "5",
    },
)
def raw_country_codes():
    return (
        spark.readStream.format("csv")
        .options(header="true", inferSchema="true")
        .schema(country_codes_schema)
        .load(COUNTRY_CODES_PATH)
    )