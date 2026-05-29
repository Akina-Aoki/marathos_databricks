"""
Gold Layer - Results Fact Table

This module creates the fact_results fact table in the Gold layer.
It contains measurable marathon race results (the "facts") that reference
dimension tables (event, athlete, country, date) in a star schema design.

Fact tables store quantitative data about business events - in this case,
each row represents one athlete's performance in one race.
"""

from pyspark import pipelines as dp

from utils.pipeline_config import DELTA_TABLE_PROPERTIES
from utils.table_names import MARATHON_RESULTS_OBT, FACT_RESULTS

from pyspark.sql.functions import col, date_format


@dp.table(
    name=FACT_RESULTS,
    comment="Fact table for marathon athlete results created from the Silver marathon OBT.",
    table_properties=DELTA_TABLE_PROPERTIES
)
def fact_results():
    """
    Create the Results fact table from the Silver layer OBT.
    
    This function extracts race result metrics and foreign keys from the Silver layer
    marathon_results_obt table to create a fact table in a star schema.

    A fact table contains:
    - Foreign keys to dimension tables (event_id, athlete_id, country_code, date_id)
    - Measures/metrics that can be aggregated (performance times, speeds, ages)
    
    This enables analytical queries like:
    - "What was the average finish time by age category?"
    - "How many finishers were there per event?"
    - "What's the trend in performance over time?"
    
    Returns:
        DataFrame: A Spark DataFrame containing race result facts with the following columns:
            - result_id: Unique identifier for this race result (primary key)
            - event_id: Foreign key to dim_event
            - athlete_id: Foreign key to dim_athlete
            - country_code_iso3: Foreign key to dim_country
            - date_id: Foreign key to dim_date (integer format: yyyyMMdd)
            - event_number_of_finishers: Total finishers in the event
            - athlete_performance: Performance time as a string (e.g., "3:45:12")
            - athlete_performance_type: Type of performance metric
            - athlete_performance_seconds: Performance time in seconds (for aggregation)
            - athlete_performance_distance: Distance covered
            - athlete_performance_unit: Unit of distance measurement
            - athlete_average_speed: Average speed as string
            - athlete_average_speed_kmh: Average speed in km/h (for aggregation)
            - athlete_age_at_event: Athlete's age when they ran the race
    """
    # Read all data from the Silver layer One Big Table (OBT)
    # This denormalized table contains all the data we need
    silver_df = spark.sql(f"""
        SELECT *
        FROM {MARATHON_RESULTS_OBT}
    """)

    # Transform into a fact table:
    # - Keep foreign keys that link to dimension tables
    # - Keep measures/metrics that can be summed, averaged, or counted
    # - Create a date surrogate key for the date dimension
    fact_results_df = (
        silver_df
        .select(
            # Primary key for this fact record
            col("result_id"),
            
            # Foreign keys - these link to dimension tables
            col("event_id"),              # Links to dim_event
            col("athlete_id"),            # Links to dim_athlete
            col("country_code_iso3"),     # Links to dim_country
            
            # Date dimension key - convert date to integer format (yyyyMMdd)
            # Example: 2023-03-15 becomes 20230315
            # This integer format is commonly used as a surrogate key for date dimensions
            date_format(col("event_start_date"), "yyyyMMdd").cast("int").alias("date_id"),
            
            # Measures/Metrics - these are the quantitative values we want to analyze
            col("event_number_of_finishers"),      # How many people finished this race
            col("athlete_performance"),             # Finish time (string format)
            col("athlete_performance_type"),        # Type of performance measurement
            col("athlete_performance_seconds"),     # Finish time in seconds (numeric for calculations)
            col("athlete_performance_distance"),    # Distance covered
            col("athlete_performance_unit"),        # Unit of distance
            col("athlete_average_speed"),           # Speed (string format)
            col("athlete_average_speed_kmh"),       # Speed in km/h (numeric for calculations)
            col("athlete_age_at_event")             # Age at time of race
        )
    )

    return fact_results_df
