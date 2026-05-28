from pyspark import pipelines as dp

from pyspark.sql.functions import col


@dp.table(
    name="marathos_catalog.gold.dim_country",
    comment="Country dimension table created from the Silver marathon OBT.",
    table_properties={
        "delta.columnMapping.mode": "name",
        "delta.minReaderVersion": "2",
        "delta.minWriterVersion": "5",
    },
)
def dim_country():
    silver_df = spark.sql("""
        SELECT *
        FROM marathos_catalog.silver.marathon_results_obt
    """)

    dim_country_df = (
        silver_df
        .select(
            col("country_code_iso3"),
            col("athlete_country"),
            col("country_name")
        )
        .dropDuplicates(["country_code_iso3"])
    )

    return dim_country_df