"""
Gold Layer - Date Dimension Table

This module creates the dim_date dimension table in the Gold layer.
It extracts unique date information from event start dates in the Silver layer OBT
and creates a date dimension with useful calendar attributes for time-based analysis.

A date dimension is a standard component in data warehouses that enables:
- Time series analysis and trend reporting
- Filtering by year, month, quarter, etc.
- Comparing performance across different time periods
"""

from pyspark import pipelines as dp

from pyspark.sql.functions import (
    col,
    year,
    month,
    dayofmonth,
    date_format,
)


@dp.table(
    name="marathos_catalog.gold.dim_date",
    comment="Date dimension table created from event_start_date in the Silver marathon OBT.",
    table_properties={
        # Enable column mapping to support column name changes
        "delta.columnMapping.mode": "name",
        # Set minimum Delta reader version
        "delta.minReaderVersion": "2",
        # Set minimum Delta writer version
        "delta.minWriterVersion": "5",
    },
)
def dim_date():
    """
    Create the Date dimension table from the Silver layer OBT.
    
    This function extracts unique event dates from the Silver layer
    marathon_results_obt table and enriches them with calendar attributes
    to create a date dimension for time-based analysis.
    
    Date dimensions typically include attributes like:
    - Surrogate key (date_id as integer)
    - Full date value
    - Calendar components (year, month, quarter, day of week)
    - Human-readable labels (month name, day name)
    
    This dimension enables time-based queries such as:
    - "How many races happened each year?"
    - "What's the average performance by month?"
    - "Show me trends over time"
    
    Returns:
        DataFrame: A Spark DataFrame containing unique date records with the following columns:
            - date_id: Surrogate key in yyyyMMdd format as integer (e.g., 20230315)
            - event_start_date: The actual date value
            - year: Calendar year (e.g., 2023)
            - month: Month number 1-12
            - month_name: Full month name (e.g., "January", "February")
            - day: Day of month, 1-31
    """
    # Read all data from the Silver layer One Big Table (OBT)
    # This table contains denormalized marathon results data
    silver_df = spark.sql("""
        SELECT *
        FROM marathos_catalog.silver.marathon_results_obt
    """)

    # Transform the data into a date dimension:
    # 1. Create a surrogate key (date_id) in integer format
    # 2. Extract useful calendar components from the date
    # 3. Remove duplicate dates to ensure one row per unique date
    dim_date_df = (
        silver_df
        .select(
            # Create date surrogate key - convert date to yyyyMMdd integer format
            # Example: 2023-03-15 becomes integer 20230315
            # Integer keys are more efficient for joins than date strings
            date_format(col("event_start_date"), "yyyyMMdd").cast("int").alias("date_id"),
            
            # Keep the original date value for reference
            col("event_start_date"),
            
            # Extract calendar components for time-based filtering and grouping
            year(col("event_start_date")).alias("year"),          # Extract year (2023)
            month(col("event_start_date")).alias("month"),        # Extract month number (1-12)
            date_format(col("event_start_date"), "MMMM").alias("month_name"),  # Get month name ("March")
            dayofmonth(col("event_start_date")).alias("day")              # Day of month, 1-31
        )
        # Deduplicate on date_id to ensure each date appears only once
        # Multiple races can happen on the same date, but we want one date record
        .dropDuplicates(["date_id"])
    )

    return dim_date_df
