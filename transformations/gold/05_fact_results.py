from pyspark import pipelines as dp

from pyspark.sql.functions import col, date_format


@dp.table(
    name="marathos_catalog.gold.fact_results",
    comment="Fact table for marathon athlete results created from the Silver marathon OBT.",
    table_properties={
        "delta.columnMapping.mode": "name",
        "delta.minReaderVersion": "2",
        "delta.minWriterVersion": "5",
    },
)
def fact_results():
    silver_df = spark.sql("""
        SELECT *
        FROM marathos_catalog.silver.marathon_results_obt
    """)

    fact_results_df = (
        silver_df
        .select(
            col("result_id"),
            col("event_id"),
            col("athlete_id"),
            col("country_code_iso3"),
            date_format(col("event_start_date"), "yyyyMMdd").cast("int").alias("date_id"),
            col("event_number_of_finishers"),
            col("athlete_performance"),
            col("athlete_performance_type"),
            col("athlete_performance_seconds"),
            col("athlete_performance_distance"),
            col("athlete_performance_unit"),
            col("athlete_average_speed"),
            col("athlete_average_speed_kmh"),
            col("athlete_age_at_event")
        )
    )

    return fact_results_df