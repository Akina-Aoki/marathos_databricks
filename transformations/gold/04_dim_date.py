from pyspark import pipelines as dp

from pyspark.sql.functions import (
    col,
    year,
    month,
    date_format,
)


@dp.table(
    name="marathos_catalog.gold.dim_date",
    comment="Date dimension table created from event_start_date in the Silver marathon OBT.",
    table_properties={
        "delta.columnMapping.mode": "name",
        "delta.minReaderVersion": "2",
        "delta.minWriterVersion": "5",
    },
)
def dim_date():
    silver_df = spark.sql("""
        SELECT *
        FROM marathos_catalog.silver.marathon_results_obt
    """)

    dim_date_df = (
        silver_df
        .select(
            date_format(col("event_start_date"), "yyyyMMdd").cast("int").alias("date_id"),
            col("event_start_date"),
            year(col("event_start_date")).alias("year"),
            month(col("event_start_date")).alias("month"),
            date_format(col("event_start_date"), "MMMM").alias("month_name")
        )
        .dropDuplicates(["date_id"])
    )

    return dim_date_df