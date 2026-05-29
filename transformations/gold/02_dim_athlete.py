"""
Gold Layer - Athlete Dimension Table

This module creates the dim_athlete dimension table in the Gold layer.
It extracts unique athlete information from the Silver layer OBT (One Big Table)
to create a dimensional model for analyzing athlete demographics and characteristics.
"""

from pyspark import pipelines as dp

from pyspark.sql.functions import col


@dp.table(
    name="marathos_catalog.gold.dim_athlete",
    comment="Athlete dimension table created from the Silver marathon OBT.",
    table_properties={
        # Enable column mapping to support column name changes
        "delta.columnMapping.mode": "name",
        # Set minimum Delta reader version
        "delta.minReaderVersion": "2",
        # Set minimum Delta writer version
        "delta.minWriterVersion": "5",
    },
)
def dim_athlete():
    """
    Create the Athlete dimension table from the Silver layer OBT.
    
    This function extracts all athlete-related attributes from the Silver layer
    marathon_results_obt table and creates a deduplicated dimension table.
    Each row represents a unique athlete with their demographic information.
    
    Returns:
        DataFrame: A Spark DataFrame containing unique athlete records with the following columns:
            - athlete_id: Unique identifier for the athlete
            - athlete_gender: Gender of the athlete
            - athlete_year_of_birth: Birth year of the athlete
            - athlete_age_category: Age category/group (e.g., "20-29", "30-39")
            - athlete_club: Running club or team affiliation
    """
    # Read all data from the Silver layer One Big Table (OBT)
    # This table contains denormalized marathon results data
    silver_df = spark.sql("""
        SELECT *
        FROM marathos_catalog.silver.marathon_results_obt
    """)

    # Transform the data into an athlete dimension:
    # 1. Select only athlete-related columns
    # 2. Remove duplicate athletes to ensure one row per unique athlete
    dim_athlete_df = (
        silver_df
        # Select athlete attributes - these describe the person who ran the race
        .select(
            col("athlete_id"),                # Primary key
            col("athlete_gender"),            # Gender (M/F)
            col("athlete_year_of_birth"),     # Birth year for age calculations
            col("athlete_age_category"),      # Age group classification
            col("athlete_club")               # Club or team affiliation
        )
        # Deduplicate on athlete_id to ensure each athlete appears only once
        # This converts denormalized data into a proper dimension table
        .dropDuplicates(["athlete_id"])
    )

    return dim_athlete_df
