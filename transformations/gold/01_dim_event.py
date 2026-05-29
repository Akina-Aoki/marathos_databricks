"""
Gold Layer - Event Dimension Table

This module creates the dim_event dimension table in the Gold layer.
It extracts unique event information from the Silver layer OBT (One Big Table)
to create a dimensional model suitable for analytics and reporting.
"""

from pyspark import pipelines as dp

from pyspark.sql.functions import col


@dp.table(
    name="marathos_catalog.gold.dim_event",
    comment="Event dimension table created from the Silver marathon OBT.",
    table_properties={
        # Enable column mapping to support column name changes
        "delta.columnMapping.mode": "name",
        # Set minimum Delta reader version
        "delta.minReaderVersion": "2",
        # Set minimum Delta writer version
        "delta.minWriterVersion": "5",
    },
)
def dim_event():
    """
    Create the Event dimension table from the Silver layer OBT.
    
    This function extracts all event-related attributes from the Silver layer
    marathon_results_obt table and creates a deduplicated dimension table.
    Each row represents a unique event (race) with its characteristics.
    
    Returns:
        DataFrame: A Spark DataFrame containing unique event records with the following columns:
            - event_id: Unique identifier for the event
            - event_name: Name of the marathon event
            - event_distance_length: Length of the race
            - event_distance_type: Type of distance measurement
            - event_distance_value: Numeric value of the distance
            - event_distance_unit: Unit of measurement (e.g., km, miles)
    """
    # Read all data from the Silver layer One Big Table (OBT)
    # This table contains denormalized marathon results data
    silver_df = spark.sql("""
        SELECT *
        FROM marathos_catalog.silver.marathon_results_obt
    """)

    # Transform the data into an event dimension:
    # 1. Select only event-related columns
    # 2. Remove duplicate events to ensure one row per unique event
    dim_event_df = (
        silver_df
        # Select event attributes - these describe the race itself
        .select(
            col("event_id"),                  # Primary key
            col("event_name"),                # Event name (e.g., "Boston Marathon")
            col("event_distance_length"),     # How long the race is
            col("event_distance_type"),       # Type classification
            col("event_distance_value"),      # Numeric distance
            col("event_distance_unit")        # Unit (kilometers, miles, etc.)
        )
        # Deduplicate on event_id to ensure each event appears only once
        # This converts denormalized data into a proper dimension table
        .dropDuplicates(["event_id"])
    )

    return dim_event_df
