"""
Gold Layer - Country Dimension Table

This module creates the dim_country dimension table in the Gold layer.
It extracts unique country information from the Silver layer OBT (One Big Table)
to create a dimensional model for geographic analysis of athlete origins.
"""

from pyspark import pipelines as dp

from pyspark.sql.functions import col


@dp.table(
    name="marathos_catalog.gold.dim_country",
    comment="Country dimension table created from the Silver marathon OBT.",
    table_properties={
        # Enable column mapping to support column name changes
        "delta.columnMapping.mode": "name",
        # Set minimum Delta reader version
        "delta.minReaderVersion": "2",
        # Set minimum Delta writer version
        "delta.minWriterVersion": "5",
    },
)
def dim_country():
    """
    Create the Country dimension table from the Silver layer OBT.
    
    This function extracts all country-related attributes from the Silver layer
    marathon_results_obt table and creates a deduplicated dimension table.
    Each row represents a unique country with its identifying codes and names.
    
    This dimension enables geographic analysis such as:
    - "How many athletes participated from each country?"
    - "What's the average performance by country?"
    - "Which countries have the most finishers?"
    
    Returns:
        DataFrame: A Spark DataFrame containing unique country records with the following columns:
            - country_code_iso3: ISO 3-letter country code (primary key, e.g., "USA", "GBR")
            - athlete_country: Country code as recorded for the athlete
            - country_name: Full name of the country (e.g., "United States", "United Kingdom")
    """
    # Read all data from the Silver layer One Big Table (OBT)
    # This table contains denormalized marathon results data
    silver_df = spark.sql("""
        SELECT *
        FROM marathos_catalog.silver.marathon_results_obt
    """)

    # Transform the data into a country dimension:
    # 1. Select only country-related columns
    # 2. Remove duplicate countries to ensure one row per unique country
    dim_country_df = (
        silver_df
        # Select country attributes - these describe where athletes are from
        .select(
            col("country_code_iso3"),     # Primary key - 3-letter ISO code
            col("athlete_country"),       # Country code as recorded for athlete
            col("country_name")           # Human-readable country name
        )
        # Deduplicate on country_code_iso3 to ensure each country appears only once
        # This converts denormalized data into a proper dimension table
        .dropDuplicates(["country_code_iso3"])
    )

    return dim_country_df
