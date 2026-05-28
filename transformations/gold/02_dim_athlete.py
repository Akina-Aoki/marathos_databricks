from pyspark import pipelines as dp

from pyspark.sql.functions import col


@dp.table(
    name="marathos_catalog.gold.dim_athlete",
    comment="Athlete dimension table created from the Silver marathon OBT.",
    table_properties={
        "delta.columnMapping.mode": "name",
        "delta.minReaderVersion": "2",
        "delta.minWriterVersion": "5",
    },
)
def dim_athlete():
    silver_df = spark.sql("""
        SELECT *
        FROM marathos_catalog.silver.marathon_results_obt
    """)

    dim_athlete_df = (
        silver_df
        .select(
            col("athlete_id"),
            col("athlete_gender"),
            col("athlete_year_of_birth"),
            col("athlete_age_category"),
            col("athlete_club")
        )
        .dropDuplicates(["athlete_id"])
    )

    return dim_athlete_df