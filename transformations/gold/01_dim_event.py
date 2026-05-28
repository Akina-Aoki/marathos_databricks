from pyspark import pipelines as dp

from pyspark.sql.functions import col


@dp.table(
    name="marathos_catalog.gold.dim_event",
    comment="Event dimension table created from the Silver marathon OBT.",
    table_properties={
        "delta.columnMapping.mode": "name",
        "delta.minReaderVersion": "2",
        "delta.minWriterVersion": "5",
    },
)
def dim_event():
    silver_df = spark.sql("""
        SELECT *
        FROM marathos_catalog.silver.marathon_results_obt
    """)

    dim_event_df = (
        silver_df
        .select(
            col("event_id"),
            col("event_name"),
            col("event_distance_length"),
            col("event_distance_type"),
            col("event_distance_value"),
            col("event_distance_unit")
        )
        .dropDuplicates(["event_id"])
    )

    return dim_event_df