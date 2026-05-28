"""
Silver Layer Transformation: Marathon Results OBT

This module creates a cleaned and enriched One Big Table (OBT) for marathon results.
It reads raw data from the Bronze layer, applies data quality rules, parses complex
date formats, enriches with country information, and outputs a clean Silver table.

The pipeline uses PySpark Declarative Pipelines (formerly DLT) for streaming transformations.
"""

from pyspark import pipelines as dp

from pyspark.sql.functions import (
    col,
    when,
    trim,
    lower,
    upper,
    lit,
    coalesce,
    regexp_extract,
    regexp_replace,
    concat,
    concat_ws,
    expr,
    length,
    sha2,
)

import re


# ------------------------------------------------------------
# Helper function: convert raw column names to snake_case
# ------------------------------------------------------------

def to_snake_case(column_name):
    """
    Convert a column name to snake_case format.
    
    Example: "Athlete Name" -> "athlete_name"
    
    Args:
        column_name: Original column name (any case/format)
    
    Returns:
        snake_case version of the column name
    """
    clean_name = column_name.strip().lower()
    clean_name = re.sub(r"[^a-z0-9]+", "_", clean_name)
    return clean_name.strip("_")


def rename_columns_to_snake_case(df):
    """
    Rename all columns in a DataFrame to snake_case.
    
    This standardizes column naming for consistency across the pipeline.
    
    Args:
        df: Input DataFrame with any column names
    
    Returns:
        DataFrame with all columns renamed to snake_case
    """
    new_columns = [to_snake_case(column) for column in df.columns]
    return df.toDF(*new_columns)


# ------------------------------------------------------------
# Silver OBT table
# ------------------------------------------------------------

@dp.table(
    name="marathos_catalog.silver.marathon_results_obt",
    comment="Cleaned and enriched marathon results OBT for the Silver layer.",
    table_properties={
        "delta.columnMapping.mode": "name",  # Allows column renaming without rewriting data
        "delta.minReaderVersion": "2",
        "delta.minWriterVersion": "5",
    },
)
def marathon_results_obt():
    """
    Main transformation function for the Silver marathon results table.
    
    This function:
    1. Reads streaming data from the Bronze layer
    2. Standardizes column names to snake_case
    3. Cleans and parses event data (dates, distances)
    4. Cleans and validates athlete data (performance, demographics)
    5. Enriches with country information
    6. Generates unique IDs for events and results
    
    Returns:
        Cleaned and enriched DataFrame ready for the Silver layer
    """

    # --------------------------------------------------------
    # Read Bronze marathon table
    # --------------------------------------------------------

    # STREAM keyword enables incremental processing of new data
    marathon_df = spark.sql("""
        SELECT *
        FROM STREAM marathos_catalog.bronze.raw_marathon_results
    """)

    marathon_df = rename_columns_to_snake_case(marathon_df)

    # --------------------------------------------------------
    # Event cleaning: year_of_event and event_name
    # --------------------------------------------------------

    marathon_df = (
        marathon_df
        .withColumn("year_of_event", col("year_of_event").cast("int"))
        .withColumn("event_name", trim(col("event_name")))
    )

    # --------------------------------------------------------
    # Event cleaning: parse event_dates
    # The raw data contains dates in 4 different formats:
    # 1. Single date: "07.12.1991"
    # 2. Same-month range: "23.-24.11.1991" (from 23rd to 24th of Nov)
    # 3. Different-month range: "30.10.-03.11.1991" (from Oct 30 to Nov 3)
    # 4. Different-year range: "31.12.1992-01.01.1993" (from Dec 31 to Jan 1)
    # --------------------------------------------------------

    # Identify which date format each row uses
    marathon_df = (
        marathon_df
        .withColumn("event_date_raw", col("event_dates"))
        .withColumn(
            "event_date_format_type",
            when(col("event_dates").rlike(r"^\d{2}\.\d{2}\.\d{4}$"), "single_date")
            .when(col("event_dates").rlike(r"^\d{2}\.-\d{2}\.\d{2}\.\d{4}$"), "date_range_same_month")
            .when(col("event_dates").rlike(r"^\d{2}\.\d{2}\.-\d{2}\.\d{2}\.\d{4}$"), "date_range_different_month")
            .when(col("event_dates").rlike(r"^\d{2}\.\d{2}\.\d{4}-\d{2}\.\d{2}\.\d{4}$"), "date_range_different_year")
            .otherwise("unknown_format")
        )
    )

    # Parse single date format (example: 07.12.1991)
    marathon_df = marathon_df.withColumn(
        "event_start_date",
        when(
            col("event_date_format_type") == "single_date",
            expr("try_to_date(event_dates, 'dd.MM.yyyy')")
        )
    )

    # Parse same-month range (example: 23.-24.11.1991)
    # Need to extract: start_day, end_day, shared month, shared year
    same_month_start_day = regexp_extract(col("event_dates"), r"^(\d{2})\.-(\d{2})\.(\d{2})\.(\d{4})$", 1)
    same_month_end_day = regexp_extract(col("event_dates"), r"^(\d{2})\.-(\d{2})\.(\d{2})\.(\d{4})$", 2)
    same_month_month = regexp_extract(col("event_dates"), r"^(\d{2})\.-(\d{2})\.(\d{2})\.(\d{4})$", 3)
    same_month_year = regexp_extract(col("event_dates"), r"^(\d{2})\.-(\d{2})\.(\d{2})\.(\d{4})$", 4)

    # Reconstruct full dates from the extracted parts
    marathon_df = (
        marathon_df
        .withColumn(
            "same_month_start_raw",
            concat(same_month_start_day, lit("."), same_month_month, lit("."), same_month_year)
        )
        .withColumn(
            "same_month_end_raw",
            concat(same_month_end_day, lit("."), same_month_month, lit("."), same_month_year)
        )
    )

    marathon_df = (
        marathon_df
        .withColumn(
            "event_start_date",
            when(
                col("event_date_format_type") == "date_range_same_month",
                expr("try_to_date(same_month_start_raw, 'dd.MM.yyyy')")
            ).otherwise(col("event_start_date"))
        )
        .withColumn(
            "event_end_date",
            when(col("event_date_format_type") == "single_date", col("event_start_date"))
            .when(
                col("event_date_format_type") == "date_range_same_month",
                expr("try_to_date(same_month_end_raw, 'dd.MM.yyyy')")
            )
        )
    )

    # Parse different-month range (example: 30.10.-03.11.1991)
    # Need to extract all parts and infer the start year
    diff_month_start_day = regexp_extract(col("event_dates"), r"^(\d{2})\.(\d{2})\.-(\d{2})\.(\d{2})\.(\d{4})$", 1)
    diff_month_start_month = regexp_extract(col("event_dates"), r"^(\d{2})\.(\d{2})\.-(\d{2})\.(\d{2})\.(\d{4})$", 2)
    diff_month_end_day = regexp_extract(col("event_dates"), r"^(\d{2})\.(\d{2})\.-(\d{2})\.(\d{2})\.(\d{4})$", 3)
    diff_month_end_month = regexp_extract(col("event_dates"), r"^(\d{2})\.(\d{2})\.-(\d{2})\.(\d{2})\.(\d{4})$", 4)
    diff_month_end_year = regexp_extract(col("event_dates"), r"^(\d{2})\.(\d{2})\.-(\d{2})\.(\d{2})\.(\d{4})$", 5)

    # If start_month > end_month, the start date was in the previous year
    # Example: "30.12.-03.01.1991" means Dec 1990 to Jan 1991
    diff_month_start_year = (
        when(
            diff_month_start_month.cast("int") > diff_month_end_month.cast("int"),
            (diff_month_end_year.cast("int") - lit(1)).cast("string")
        )
        .otherwise(diff_month_end_year)
    )

    marathon_df = (
        marathon_df
        .withColumn(
            "diff_month_start_raw",
            concat(diff_month_start_day, lit("."), diff_month_start_month, lit("."), diff_month_start_year)
        )
        .withColumn(
            "diff_month_end_raw",
            concat(diff_month_end_day, lit("."), diff_month_end_month, lit("."), diff_month_end_year)
        )
    )

    marathon_df = (
        marathon_df
        .withColumn(
            "event_start_date",
            when(
                col("event_date_format_type") == "date_range_different_month",
                expr("try_to_date(diff_month_start_raw, 'dd.MM.yyyy')")
            ).otherwise(col("event_start_date"))
        )
        .withColumn(
            "event_end_date",
            when(
                col("event_date_format_type") == "date_range_different_month",
                expr("try_to_date(diff_month_end_raw, 'dd.MM.yyyy')")
            ).otherwise(col("event_end_date"))
        )
    )

    # Parse different-year range (example: 31.12.1992-01.01.1993)
    # Both dates are fully specified, so just extract all parts
    diff_year_start_day = regexp_extract(col("event_dates"), r"^(\d{2})\.(\d{2})\.(\d{4})-(\d{2})\.(\d{2})\.(\d{4})$", 1)
    diff_year_start_month = regexp_extract(col("event_dates"), r"^(\d{2})\.(\d{2})\.(\d{4})-(\d{2})\.(\d{2})\.(\d{4})$", 2)
    diff_year_start_year = regexp_extract(col("event_dates"), r"^(\d{2})\.(\d{2})\.(\d{4})-(\d{2})\.(\d{2})\.(\d{4})$", 3)
    diff_year_end_day = regexp_extract(col("event_dates"), r"^(\d{2})\.(\d{2})\.(\d{4})-(\d{2})\.(\d{2})\.(\d{4})$", 4)
    diff_year_end_month = regexp_extract(col("event_dates"), r"^(\d{2})\.(\d{2})\.(\d{4})-(\d{2})\.(\d{2})\.(\d{4})$", 5)
    diff_year_end_year = regexp_extract(col("event_dates"), r"^(\d{2})\.(\d{2})\.(\d{4})-(\d{2})\.(\d{2})\.(\d{4})$", 6)

    marathon_df = (
        marathon_df
        .withColumn(
            "diff_year_start_raw",
            concat(diff_year_start_day, lit("."), diff_year_start_month, lit("."), diff_year_start_year)
        )
        .withColumn(
            "diff_year_end_raw",
            concat(diff_year_end_day, lit("."), diff_year_end_month, lit("."), diff_year_end_year)
        )
    )

    marathon_df = (
        marathon_df
        .withColumn(
            "event_start_date",
            when(
                col("event_date_format_type") == "date_range_different_year",
                expr("try_to_date(diff_year_start_raw, 'dd.MM.yyyy')")
            ).otherwise(col("event_start_date"))
        )
        .withColumn(
            "event_end_date",
            when(
                col("event_date_format_type") == "date_range_different_year",
                expr("try_to_date(diff_year_end_raw, 'dd.MM.yyyy')")
            ).otherwise(col("event_end_date"))
        )
    )

    # Remove rows where event dates could not be parsed
    marathon_df = marathon_df.filter(
        col("event_start_date").isNotNull()
        & col("event_end_date").isNotNull()
    )

    # --------------------------------------------------------
    # Event cleaning: distance/length
    # Events can be distance-based (42km) or time-based (24h)
    # --------------------------------------------------------

    # Classify each event by its distance/time format
    marathon_df = marathon_df.withColumn(
        "event_distance_type",
        when(lower(trim(col("event_distance_length"))).rlike(r"^[0-9]+(\.[0-9]+)?km$"), "distance_km")
        .when(lower(trim(col("event_distance_length"))).rlike(r"^[0-9]+(\.[0-9]+)?mi$"), "distance_mi")
        .when(lower(trim(col("event_distance_length"))).rlike(r"^[0-9]+h$"), "time_hours")
        .when(lower(trim(col("event_distance_length"))).rlike(r"^[0-9]+(\.[0-9]+)?k$"), "distance_k_needs_standardization")
        .when(lower(trim(col("event_distance_length"))).rlike(r"^[0-9]+d$"), "days_invalid")
        .when(lower(trim(col("event_distance_length"))).contains("etappen"), "multi_stage_invalid")
        .otherwise("invalid_or_unknown")
    )

    # Keep only valid event types (distance or time-based)
    marathon_df = marathon_df.filter(
        col("event_distance_type").isin(
            "distance_km",
            "distance_mi",
            "time_hours",
            "distance_k_needs_standardization"
        )
    )

    # Standardize "50k" to "50km"
    marathon_df = marathon_df.withColumn(
        "event_distance_length",
        when(
            col("event_distance_type") == "distance_k_needs_standardization",
            regexp_replace(lower(trim(col("event_distance_length"))), "k$", "km")
        ).otherwise(lower(trim(col("event_distance_length"))))
    )

    marathon_df = marathon_df.withColumn(
        "event_distance_type",
        when(col("event_distance_type") == "distance_k_needs_standardization", "distance_km")
        .otherwise(col("event_distance_type"))
    )

    # Extract numeric value and unit separately
    marathon_df = (
        marathon_df
        .withColumn(
            "event_distance_value",
            regexp_extract(col("event_distance_length"), r"^([0-9]+(\.[0-9]+)?)", 1).cast("double")
        )
        .withColumn(
            "event_distance_unit",
            regexp_extract(col("event_distance_length"), r"(km|mi|h)$", 1)
        )
    )

    # Remove events with no finishers (data quality check)
    marathon_df = marathon_df.filter(
        col("event_number_of_finishers").isNotNull()
        & (col("event_number_of_finishers") > 0)
    )

    # --------------------------------------------------------
    # Athlete cleaning: performance
    # Performance can be time (for distance events) or distance (for time events)
    # --------------------------------------------------------

    # Identify the performance format
    marathon_df = marathon_df.withColumn(
        "athlete_performance_type",
        when(col("athlete_performance").isNull(), "null")
        .when(trim(col("athlete_performance")).rlike(r"^[0-9]+:[0-9]{2}:[0-9]{2} h$"), "time_hours")
        .when(trim(col("athlete_performance")).rlike(r"^[0-9]+d [0-9]{2}:[0-9]{2}:[0-9]{2} h$"), "time_days_hours")
        .when(trim(col("athlete_performance")).rlike(r"^[0-9]+(\.[0-9]+)? km$"), "distance_km")
        .when(trim(col("athlete_performance")).rlike(r"^[0-9]+(\.[0-9]+)? mi$"), "distance_mi")
        .otherwise("unknown")
    )

    # Remove rows with null or unknown performance
    marathon_df = marathon_df.filter(
        ~col("athlete_performance_type").isin("null", "unknown")
    )

    # Validate that event type matches performance type:
    # - Distance events (km/mi) should have time performance
    # - Time events (hours) should have distance performance
    marathon_df = marathon_df.filter(
        (
            col("event_distance_unit").isin("km", "mi")
            & col("athlete_performance_type").isin("time_hours", "time_days_hours")
        )
        |
        (
            (col("event_distance_unit") == "h")
            & col("athlete_performance_type").isin("distance_km", "distance_mi")
        )
    )

    # Parse time performance (example: "3:45:12 h")
    performance_hours = regexp_extract(col("athlete_performance"), r"^([0-9]+):([0-9]{2}):([0-9]{2}) h$", 1)
    performance_minutes = regexp_extract(col("athlete_performance"), r"^([0-9]+):([0-9]{2}):([0-9]{2}) h$", 2)
    performance_seconds = regexp_extract(col("athlete_performance"), r"^([0-9]+):([0-9]{2}):([0-9]{2}) h$", 3)

    # Parse time with days (example: "2d 14:30:00 h")
    performance_days = regexp_extract(col("athlete_performance"), r"^([0-9]+)d ([0-9]{2}):([0-9]{2}):([0-9]{2}) h$", 1)
    performance_day_hours = regexp_extract(col("athlete_performance"), r"^([0-9]+)d ([0-9]{2}):([0-9]{2}):([0-9]{2}) h$", 2)
    performance_day_minutes = regexp_extract(col("athlete_performance"), r"^([0-9]+)d ([0-9]{2}):([0-9]{2}):([0-9]{2}) h$", 3)
    performance_day_seconds = regexp_extract(col("athlete_performance"), r"^([0-9]+)d ([0-9]{2}):([0-9]{2}):([0-9]{2}) h$", 4)

    # Convert all time formats to total seconds for easy comparison
    marathon_df = marathon_df.withColumn(
        "athlete_performance_seconds",
        when(
            col("athlete_performance_type") == "time_hours",
            performance_hours.cast("int") * 3600
            + performance_minutes.cast("int") * 60
            + performance_seconds.cast("int")
        ).when(
            col("athlete_performance_type") == "time_days_hours",
            performance_days.cast("int") * 86400
            + performance_day_hours.cast("int") * 3600
            + performance_day_minutes.cast("int") * 60
            + performance_day_seconds.cast("int")
        )
    )

    # Parse distance performance (for time-based events)
    marathon_df = marathon_df.withColumn(
        "athlete_performance_distance",
        when(
            col("athlete_performance_type") == "distance_km",
            regexp_extract(col("athlete_performance"), r"^([0-9]+(\.[0-9]+)?) km$", 1).cast("double")
        ).when(
            col("athlete_performance_type") == "distance_mi",
            regexp_extract(col("athlete_performance"), r"^([0-9]+(\.[0-9]+)?) mi$", 1).cast("double")
        )
    )

    # Store the performance unit for downstream analysis
    marathon_df = marathon_df.withColumn(
        "athlete_performance_unit",
        when(col("athlete_performance_type").isin("time_hours", "time_days_hours"), "seconds")
        .when(col("athlete_performance_type") == "distance_km", "km")
        .when(col("athlete_performance_type") == "distance_mi", "mi")
    )

    # --------------------------------------------------------
    # Athlete cleaning: club, country, birth year, age, gender
    # --------------------------------------------------------

    # Clean club names: remove leading asterisks and whitespace
    marathon_df = marathon_df.withColumn(
        "athlete_club",
        trim(
            regexp_replace(
                trim(coalesce(col("athlete_club"), lit("unknown"))),
                r"^\*+\s*",
                ""
            )
        )
    )

    # Replace empty club names with "unknown"
    marathon_df = marathon_df.withColumn(
        "athlete_club",
        when(length(trim(col("athlete_club"))) == 0, lit("unknown"))
        .otherwise(col("athlete_club"))
    )

    # Standardize country codes to uppercase
    marathon_df = (
        marathon_df
        .withColumn("athlete_country", upper(trim(col("athlete_country"))))
        .filter(col("athlete_country").isNotNull())
    )

    # Fix data quality issue: year 1193 is clearly wrong (likely 1993)
    marathon_df = marathon_df.withColumn(
        "athlete_year_of_birth",
        when(col("athlete_year_of_birth") == 1193, None)
        .otherwise(col("athlete_year_of_birth").cast("int"))
    )

    # Calculate athlete age at the time of the event
    marathon_df = marathon_df.withColumn(
        "athlete_age_at_event",
        col("year_of_event") - col("athlete_year_of_birth")
    )

    # Remove unrealistic ages (< 5 or > 100)
    marathon_df = marathon_df.withColumn(
        "athlete_age_at_event",
        when(
            (col("athlete_age_at_event") < 5)
            | (col("athlete_age_at_event") > 100),
            None
        ).otherwise(col("athlete_age_at_event"))
    )

    # Standardize age category to uppercase
    marathon_df = marathon_df.withColumn(
        "athlete_age_category",
        when(
            col("athlete_age_category").isNull()
            | (length(trim(col("athlete_age_category"))) == 0),
            lit("unknown")
        ).otherwise(upper(trim(col("athlete_age_category"))))
    )

    # Standardize gender to uppercase
    marathon_df = (
        marathon_df
        .withColumn("athlete_gender", upper(trim(col("athlete_gender"))))
        .filter(col("athlete_gender").isNotNull())
    )

    marathon_df = marathon_df.withColumn(
        "athlete_id",
        col("athlete_id").cast("int")
    )

    # --------------------------------------------------------
    # Athlete cleaning: average speed
    # Some speeds are stored in m/h instead of km/h (need to divide by 1000)
    # --------------------------------------------------------

    marathon_df = marathon_df.withColumn(
        "athlete_average_speed_numeric",
        expr("try_cast(athlete_average_speed as double)")
    )

    # Convert speeds > 1000 from m/h to km/h
    marathon_df = marathon_df.withColumn(
        "athlete_average_speed_kmh",
        when(
            col("athlete_average_speed_numeric") > 1000,
            col("athlete_average_speed_numeric") / 1000
        ).otherwise(col("athlete_average_speed_numeric"))
    )

    # Remove invalid speeds (null or zero)
    marathon_df = marathon_df.filter(
        col("athlete_average_speed_kmh").isNotNull()
        & (col("athlete_average_speed_kmh") > 0)
    )

    marathon_df = marathon_df.drop("athlete_average_speed_numeric")

    # --------------------------------------------------------
    # Country mapping enrichment
    # Join with country reference table to get ISO3 codes and full names
    # --------------------------------------------------------

    country_df = spark.sql("""
        SELECT *
        FROM marathos_catalog.bronze.raw_country_codes
    """)

    country_df = (
        country_df
        .withColumn("athlete_country_code", upper(trim(col("athlete_country_code"))))
        .withColumn("country_code_iso3", upper(trim(col("country_code_iso3"))))
        .withColumn("country_name", trim(col("country_name")))
        .filter(
            col("athlete_country_code").isNotNull()
            & col("country_code_iso3").isNotNull()
            & col("country_name").isNotNull()
        )
    )

    # Inner join: only keep records with valid country mappings
    marathon_df = (
        marathon_df
        .join(
            country_df,
            marathon_df["athlete_country"] == country_df["athlete_country_code"],
            "inner"
        )
        .drop("athlete_country_code")
    )

    # --------------------------------------------------------
    # Remove exact duplicate result rows before creating IDs
    # --------------------------------------------------------

    dedup_columns = [
        "year_of_event",
        "event_date_raw",
        "event_name",
        "event_distance_length",
        "event_number_of_finishers",
        "athlete_performance",
        "athlete_club",
        "athlete_country",
        "athlete_year_of_birth",
        "athlete_gender",
        "athlete_age_category",
        "athlete_average_speed",
        "athlete_id",
        "country_code_iso3",
        "country_name",
    ]

    marathon_df = marathon_df.dropDuplicates(dedup_columns)

    # --------------------------------------------------------
    # Create deterministic IDs for dimensional modelling
    # Using SHA2 hash ensures consistent IDs across pipeline runs
    # Dense rank is avoided because this is a streaming table
    # --------------------------------------------------------

    marathon_df = (
        marathon_df
        .withColumn(
            "event_id",
            sha2(
                concat_ws(
                    "||",
                    col("event_name"),
                    col("event_distance_length")
                ),
                256
            )
        )
        .withColumn(
            "result_id",
            sha2(
                concat_ws(
                    "||",
                    col("event_name"),
                    col("event_date_raw"),
                    col("event_distance_length"),
                    col("event_number_of_finishers").cast("string"),
                    col("athlete_id").cast("string"),
                    col("athlete_country"),
                    col("country_code_iso3"),
                    col("country_name"),
                    col("athlete_year_of_birth").cast("string"),
                    col("athlete_gender"),
                    col("athlete_age_category"),
                    col("athlete_club"),
                    col("athlete_performance"),
                    col("athlete_average_speed")
                ),
                256
            )
        )
    )

    # --------------------------------------------------------
    # Drop temporary date parsing helper columns
    # --------------------------------------------------------

    marathon_df = marathon_df.drop(
        "same_month_start_raw",
        "same_month_end_raw",
        "diff_month_start_raw",
        "diff_month_end_raw",
        "diff_year_start_raw",
        "diff_year_end_raw"
    )

    # --------------------------------------------------------
    # Return final Silver OBT
    # --------------------------------------------------------

    return marathon_df
