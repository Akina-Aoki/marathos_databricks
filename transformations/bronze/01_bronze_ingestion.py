"""
Bronze Layer Ingestion Pipeline

This module ingests raw CSV files from Unity Catalog volumes into Bronze layer tables.
Bronze tables contain unprocessed data as-is from the source.
"""

from pyspark import pipelines as dp

from utils.pipeline_config import DELTA_TABLE_PROPERTIES
from utils.table_names import (
    RAW_COUNTRY_CODES,
    RAW_MARATHON_RESULTS,
    RAW_VOLUME_BASE_DIR,
)

# ------------------------------------------------------------
# Base paths in Unity Catalog raw volume
# ------------------------------------------------------------

BASE_DIR = RAW_VOLUME_BASE_DIR

MARATHON_DATA_PATH = f"{BASE_DIR}/data"
MARATHON_SAMPLE_FILE = f"{MARATHON_DATA_PATH}/TWO_CENTURIES_OF_UM_RACES.csv"

COUNTRY_CODES_PATH = f"{BASE_DIR}/country_iso"
COUNTRY_CODES_SAMPLE_FILE = f"{COUNTRY_CODES_PATH}/marathos_country_codes.csv"


# ------------------------------------------------------------
# Parse schemas from CSV files
# ------------------------------------------------------------
# Streaming reads require a predefined schema because Spark cannot
# infer schema continuously while streaming. We read a sample file
# once to extract the schema, then use it for the streaming read.

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
    # Fully-qualified table name: catalog.schema.table
    name=RAW_MARATHON_RESULTS,
    comment="Raw ultra-marathon race results ingested into the Bronze layer.",
    table_properties=DELTA_TABLE_PROPERTIES,
)
def raw_marathon_results():
    """
    Ingest marathon race results from CSV files using streaming read.
    
    This function defines a streaming table that automatically processes new files
    as they arrive in the source directory.
    
    Returns:
        Streaming DataFrame containing raw marathon data from all CSV files
        in the marathon data directory.
    """
    return (
        spark.readStream.format("csv")  # Streaming read for incremental processing
        .options(header="true", inferSchema="true")
        .schema(marathon_schema)  # Use predefined schema for streaming
        .load(MARATHON_DATA_PATH)  # Read all CSV files from directory
    )


# ------------------------------------------------------------
# Bronze table 2: Raw country code mapping
# ------------------------------------------------------------

@dp.table(
    # Fully-qualified table name: catalog.schema.table
    name=RAW_COUNTRY_CODES,
    comment="Raw Marathos country code mapping ingested into the Bronze layer.",
    table_properties=DELTA_TABLE_PROPERTIES,
)
def raw_country_codes():
    """
    Ingest country code mappings from CSV files using streaming read.
    
    This function defines a streaming table that automatically processes new files
    as they arrive in the source directory.
    
    Returns:
        Streaming DataFrame containing country code reference data from all
        CSV files in the country codes directory.
    """
    return (
        spark.readStream.format("csv")  # Streaming read for incremental processing
        .options(header="true", inferSchema="true")
        .schema(country_codes_schema)  # Use predefined schema for streaming
        .load(COUNTRY_CODES_PATH)  # Read all CSV files from directory
    )
