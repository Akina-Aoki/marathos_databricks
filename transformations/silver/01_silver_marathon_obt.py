"""
Silver Layer Marathon Results OBT (One Big Table)

This module transforms raw marathon data from the Bronze layer into a cleaned,
enriched Silver layer table that is ready for analytics and reporting.

Key transformations:
- Parses complex date formats (single dates, date ranges across months/years)
- Standardizes distance measurements (km, miles, hours)
- Converts athlete performance into comparable metrics (seconds, distances)
- Calculates accurate average speeds from cleaned data
- Enriches athlete records with country information
- Generates deterministic IDs for dimensional modeling
- Filters out invalid, incomplete, or unrealistic data

The output is a streaming table that incrementally processes new marathon results
as they arrive in the Bronze layer.
"""

from pyspark import pipelines as dp

from utils.column_helpers import rename_columns_to_snake_case
from utils.pipeline_config import DELTA_TABLE_PROPERTIES
from utils.table_names import (
    MARATHON_RESULTS_OBT,
    RAW_COUNTRY_CODES,
    RAW_MARATHON_RESULTS,
)

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
    round,
)



# ------------------------------------------------------------
# Silver OBT table
# ------------------------------------------------------------

@dp.table(
    name=MARATHON_RESULTS_OBT,
    comment="Cleaned and enriched marathon results OBT for the Silver layer.",
    table_properties=DELTA_TABLE_PROPERTIES
)
def marathon_results_obt():
    """
    Create the Silver layer marathon results One Big Table (OBT).
    
    This function performs comprehensive data cleaning and enrichment on raw marathon
    data. The output is a streaming table that processes new records incrementally
    as they arrive in the Bronze layer.
    
    The transformation pipeline includes:
    1. Column name standardization
    2. Event metadata cleaning (dates, distances, finisher counts)
    3. Athlete performance parsing and standardization
    4. Athlete demographic cleaning (club, country, age, gender)
    5. Average speed calculation from cleaned metrics
    6. Country code enrichment via lookup table
    7. Deduplication of exact duplicate records
    8. Generation of deterministic IDs for downstream dimensional modeling
    
    Data Quality Rules Applied:
    - Event dates must be parseable and valid
    - Distances must be in recognized formats (km, mi, or hours)
    - Performance metrics must match event type (time for distance, distance for timed)
    - Athlete age must be between 5 and 100 years
    - Average speed must be between 0 and 50 km/h (filters outliers)
    - All core demographic fields must be present (country, gender, age)
    
    Returns:
        DataFrame: Cleaned and enriched marathon results with the following key columns:
            - event_id: Deterministic hash based on event name and distance
            - result_id: Unique hash for each result record
            - event_start_date, event_end_date: Parsed event dates
            - event_distance_value, event_distance_unit: Standardized distance
            - athlete_performance_seconds: Time taken (for distance events)
            - athlete_performance_distance: Distance covered (for timed events)
            - athlete_average_speed_kmh: Recalculated speed in km/h
            - athlete_age_at_event: Calculated age at event time
            - country_name, country_code_iso3: Enriched country information
    """

    # --------------------------------------------------------
    # Read Bronze marathon table
    # --------------------------------------------------------
    # Using STREAM keyword to read from the Bronze streaming table incrementally.
    # This means only new/changed records are processed, not the entire history.

    marathon_df = spark.sql(f"""
        SELECT *
        FROM STREAM {RAW_MARATHON_RESULTS}
    """)

    # Standardize all column names to snake_case for consistency
    marathon_df = rename_columns_to_snake_case(marathon_df)

    # --------------------------------------------------------
    # Event cleaning: year_of_event and event_name
    # --------------------------------------------------------
    # Basic cleaning of event metadata fields

    marathon_df = (
        marathon_df
        .withColumn("year_of_event", col("year_of_event").cast("int"))
        .withColumn("event_name", trim(col("event_name")))  # Remove leading/trailing whitespace
    )

    # --------------------------------------------------------
    # Event cleaning: parse event_dates
    # --------------------------------------------------------
    # The raw event_dates field contains multiple date formats:
    # 1. Single date: "07.12.1991"
    # 2. Same month range: "23.-24.11.1991" (event spans Nov 23-24)
    # 3. Different month range: "30.10.-03.11.1991" (event spans Oct 30 - Nov 3)
    # 4. Different year range: "31.12.1992-01.01.1993" (event spans New Year)
    #
    # We need to parse each format and extract event_start_date and event_end_date

    # First, identify which format each row uses
    marathon_df = (
        marathon_df
        .withColumn("event_date_raw", col("event_dates"))  # Keep original for reference
        .withColumn(
            "event_date_format_type",
            when(col("event_dates").rlike(r"^\d{2}\.\d{2}\.\d{4}$"), "single_date")
            .when(col("event_dates").rlike(r"^\d{2}\.-\d{2}\.\d{2}\.\d{4}$"), "date_range_same_month")
            .when(col("event_dates").rlike(r"^\d{2}\.\d{2}\.-\d{2}\.\d{2}\.\d{4}$"), "date_range_different_month")
            .when(col("event_dates").rlike(r"^\d{2}\.\d{2}\.\d{4}-\d{2}\.\d{2}\.\d{4}$"), "date_range_different_year")
            .otherwise("unknown_format")
        )
    )

    # --------------------------------------------------------
    # Format 1: Single date (e.g., "07.12.1991")
    # --------------------------------------------------------
    # For single-day events, start_date and end_date are the same

    marathon_df = marathon_df.withColumn(
        "event_start_date",
        when(
            col("event_date_format_type") == "single_date",
            expr("try_to_date(event_dates, 'dd.MM.yyyy')")  # Parse as DD.MM.YYYY
        )
    )

    # --------------------------------------------------------
    # Format 2: Same-month range (e.g., "23.-24.11.1991")
    # --------------------------------------------------------
    # Extract the two days and the shared month/year, then construct full dates

    # Extract day 1, day 2, month, and year from the pattern "DD.-DD.MM.YYYY"
    same_month_start_day = regexp_extract(col("event_dates"), r"^(\d{2})\.-(\d{2})\.(\d{2})\.(\d{4})$", 1)
    same_month_end_day = regexp_extract(col("event_dates"), r"^(\d{2})\.-(\d{2})\.(\d{2})\.(\d{4})$", 2)
    same_month_month = regexp_extract(col("event_dates"), r"^(\d{2})\.-(\d{2})\.(\d{2})\.(\d{4})$", 3)
    same_month_year = regexp_extract(col("event_dates"), r"^(\d{2})\.-(\d{2})\.(\d{2})\.(\d{4})$", 4)

    # Build complete date strings for start and end dates
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

    # Parse the constructed date strings
    marathon_df = (
        marathon_df
        .withColumn(
            "event_start_date",
            when(
                col("event_date_format_type") == "date_range_same_month",
                expr("try_to_date(same_month_start_raw, 'dd.MM.yyyy')")
            ).otherwise(col("event_start_date"))  # Keep previous value if not this format
        )
        .withColumn(
            "event_end_date",
            when(col("event_date_format_type") == "single_date", col("event_start_date"))  # Single day events
            .when(
                col("event_date_format_type") == "date_range_same_month",
                expr("try_to_date(same_month_end_raw, 'dd.MM.yyyy')")
            )
        )
    )

    # --------------------------------------------------------
    # Format 3: Different-month range (e.g., "30.10.-03.11.1991")
    # --------------------------------------------------------
    # Extract all components and handle year inference
    # If start month > end month, the start date is in the previous year

    diff_month_start_day = regexp_extract(col("event_dates"), r"^(\d{2})\.(\d{2})\.-(\d{2})\.(\d{2})\.(\d{4})$", 1)
    diff_month_start_month = regexp_extract(col("event_dates"), r"^(\d{2})\.(\d{2})\.-(\d{2})\.(\d{2})\.(\d{4})$", 2)
    diff_month_end_day = regexp_extract(col("event_dates"), r"^(\d{2})\.(\d{2})\.-(\d{2})\.(\d{2})\.(\d{4})$", 3)
    diff_month_end_month = regexp_extract(col("event_dates"), r"^(\d{2})\.(\d{2})\.-(\d{2})\.(\d{2})\.(\d{4})$", 4)
    diff_month_end_year = regexp_extract(col("event_dates"), r"^(\d{2})\.(\d{2})\.-(\d{2})\.(\d{2})\.(\d{4})$", 5)

    # Infer start year: if start month (e.g., Oct=10) > end month (e.g., Nov=11 is false),
    # but if start month=12 and end month=1, then start year = end year - 1
    diff_month_start_year = (
        when(
            diff_month_start_month.cast("int") > diff_month_end_month.cast("int"),
            (diff_month_end_year.cast("int") - lit(1)).cast("string")
        )
        .otherwise(diff_month_end_year)
    )

    # Build complete date strings
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

    # Parse the dates
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

    # --------------------------------------------------------
    # Format 4: Different-year range (e.g., "31.12.1992-01.01.1993")
    # --------------------------------------------------------
    # Both dates are fully specified, so extraction is straightforward

    diff_year_start_day = regexp_extract(col("event_dates"), r"^(\d{2})\.(\d{2})\.(\d{4})-(\d{2})\.(\d{2})\.(\d{4})$", 1)
    diff_year_start_month = regexp_extract(col("event_dates"), r"^(\d{2})\.(\d{2})\.(\d{4})-(\d{2})\.(\d{2})\.(\d{4})$", 2)
    diff_year_start_year = regexp_extract(col("event_dates"), r"^(\d{2})\.(\d{2})\.(\d{4})-(\d{2})\.(\d{2})\.(\d{4})$", 3)
    diff_year_end_day = regexp_extract(col("event_dates"), r"^(\d{2})\.(\d{2})\.(\d{4})-(\d{2})\.(\d{2})\.(\d{4})$", 4)
    diff_year_end_month = regexp_extract(col("event_dates"), r"^(\d{2})\.(\d{2})\.(\d{4})-(\d{2})\.(\d{2})\.(\d{4})$", 5)
    diff_year_end_year = regexp_extract(col("event_dates"), r"^(\d{2})\.(\d{2})\.(\d{4})-(\d{2})\.(\d{2})\.(\d{4})$", 6)

    # Build complete date strings
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

    # Parse the dates
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

    # --------------------------------------------------------
    # Data Quality: Remove rows with unparseable dates
    # --------------------------------------------------------
    # If we couldn't parse the dates, we can't use the record for time-based analysis

    marathon_df = marathon_df.filter(
        col("event_start_date").isNotNull()
        & col("event_end_date").isNotNull()
    )

    # --------------------------------------------------------
    # Event cleaning: distance/length
    # --------------------------------------------------------
    # The raw event_distance_length field contains various formats:
    # - "42.195km" or "42.195 km" (distance in kilometers)
    # - "26.2mi" or "26.2 mi" (distance in miles)
    # - "24h" (fixed-time event, e.g., 24-hour race)
    # - "50k" (shorthand for "50km", needs standardization)
    # - "7d" (multi-day events - invalid for our analysis)
    # - "3 etappen" (multi-stage races - invalid for our analysis)
    
    # First, classify each distance format
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

    # Filter out invalid event types (multi-day and multi-stage events)
    marathon_df = marathon_df.filter(
        col("event_distance_type").isin(
            "distance_km",
            "distance_mi",
            "time_hours",
            "distance_k_needs_standardization"
        )
    )

    # Standardize "k" suffix to "km" (e.g., "50k" → "50km")
    marathon_df = marathon_df.withColumn(
        "event_distance_length",
        when(
            col("event_distance_type") == "distance_k_needs_standardization",
            regexp_replace(lower(trim(col("event_distance_length"))), "k$", "km")
        ).otherwise(lower(trim(col("event_distance_length"))))
    )

    # Update the type after standardization
    marathon_df = marathon_df.withColumn(
        "event_distance_type",
        when(col("event_distance_type") == "distance_k_needs_standardization", "distance_km")
        .otherwise(col("event_distance_type"))
    )

    # Extract numeric value and unit into separate columns
    # This makes it easier to do calculations and comparisons later
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

    # --------------------------------------------------------
    # Data Quality: Remove events with invalid finisher counts
    # --------------------------------------------------------
    # Events must have at least one finisher to be valid

    marathon_df = marathon_df.filter(
        col("event_number_of_finishers").isNotNull()
        & (col("event_number_of_finishers") > 0)
    )

    # --------------------------------------------------------
    # Athlete cleaning: performance
    # --------------------------------------------------------
    # The athlete_performance field contains different formats depending on event type:
    # 
    # For distance events (km/mi):
    # - "2:15:30 h" (time taken: 2 hours, 15 minutes, 30 seconds)
    # - "1d 05:30:00 h" (time taken: 1 day, 5 hours, 30 minutes - for ultra marathons)
    # 
    # For fixed-time events (e.g., 24-hour races):
    # - "150.5 km" (distance covered in the fixed time)
    # - "93.2 mi" (distance covered in miles)
    
    # First, classify each performance format
    marathon_df = marathon_df.withColumn(
        "athlete_performance_type",
        when(col("athlete_performance").isNull(), "null")
        .when(trim(col("athlete_performance")).rlike(r"^[0-9]+:[0-9]{2}:[0-9]{2} h$"), "time_hours")
        .when(trim(col("athlete_performance")).rlike(r"^[0-9]+d [0-9]{2}:[0-9]{2}:[0-9]{2} h$"), "time_days_hours")
        .when(trim(col("athlete_performance")).rlike(r"^[0-9]+(\.[0-9]+)? km$"), "distance_km")
        .when(trim(col("athlete_performance")).rlike(r"^[0-9]+(\.[0-9]+)? mi$"), "distance_mi")
        .otherwise("unknown")
    )

    # Remove records with null or unknown performance formats
    marathon_df = marathon_df.filter(
        ~col("athlete_performance_type").isin("null", "unknown")
    )

    # --------------------------------------------------------
    # Data Quality: Validate performance type matches event type
    # --------------------------------------------------------
    # Distance events (km/mi) should have time-based performance
    # Fixed-time events (hours) should have distance-based performance
    
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

    # --------------------------------------------------------
    # Parse time-based performance (for distance events)
    # --------------------------------------------------------
    # Extract hours, minutes, seconds from "HH:MM:SS h" format
    
    performance_hours = regexp_extract(col("athlete_performance"), r"^([0-9]+):([0-9]{2}):([0-9]{2}) h$", 1)
    performance_minutes = regexp_extract(col("athlete_performance"), r"^([0-9]+):([0-9]{2}):([0-9]{2}) h$", 2)
    performance_seconds = regexp_extract(col("athlete_performance"), r"^([0-9]+):([0-9]{2}):([0-9]{2}) h$", 3)

    # Extract days, hours, minutes, seconds from "Dd HH:MM:SS h" format (for ultra marathons)
    performance_days = regexp_extract(col("athlete_performance"), r"^([0-9]+)d ([0-9]{2}):([0-9]{2}):([0-9]{2}) h$", 1)
    performance_day_hours = regexp_extract(col("athlete_performance"), r"^([0-9]+)d ([0-9]{2}):([0-9]{2}):([0-9]{2}) h$", 2)
    performance_day_minutes = regexp_extract(col("athlete_performance"), r"^([0-9]+)d ([0-9]{2}):([0-9]{2}):([0-9]{2}) h$", 3)
    performance_day_seconds = regexp_extract(col("athlete_performance"), r"^([0-9]+)d ([0-9]{2}):([0-9]{2}):([0-9]{2}) h$", 4)

    # Convert all time-based performance to total seconds for easier calculations
    # 1 day = 86,400 seconds, 1 hour = 3,600 seconds, 1 minute = 60 seconds
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

    # --------------------------------------------------------
    # Parse distance-based performance (for fixed-time events)
    # --------------------------------------------------------
    # Extract distance value from "XXX.X km" or "XXX.X mi" format
    
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

    # Track the unit of the performance metric for later speed calculations
    marathon_df = marathon_df.withColumn(
        "athlete_performance_unit",
        when(col("athlete_performance_type").isin("time_hours", "time_days_hours"), "seconds")
        .when(col("athlete_performance_type") == "distance_km", "km")
        .when(col("athlete_performance_type") == "distance_mi", "mi")
    )

    # --------------------------------------------------------
    # Athlete cleaning: club, country, birth year, age, gender
    # --------------------------------------------------------

    # Clean athlete club names:
    # - Remove leading asterisks (used in raw data for annotations)
    # - Replace empty/null values with "unknown"
    marathon_df = marathon_df.withColumn(
        "athlete_club",
        trim(
            regexp_replace(
                trim(coalesce(col("athlete_club"), lit("unknown"))),
                r"^\*+\s*",  # Remove leading asterisks and spaces
                ""
            )
        )
    )

    marathon_df = marathon_df.withColumn(
        "athlete_club",
        when(length(trim(col("athlete_club"))) == 0, lit("unknown"))
        .otherwise(col("athlete_club"))
    )

    # Standardize country codes to uppercase and remove nulls
    # Country codes are critical for joining with the country lookup table
    marathon_df = (
        marathon_df
        .withColumn("athlete_country", upper(trim(col("athlete_country"))))
        .filter(col("athlete_country").isNotNull())
    )

    # --------------------------------------------------------
    # Data Quality: Fix and validate birth year
    # --------------------------------------------------------
    # There's a known data quality issue where 1193 appears (typo for 1993)
    
    marathon_df = marathon_df.withColumn(
        "athlete_year_of_birth",
        when(col("athlete_year_of_birth") == 1193, None)  # Treat as invalid
        .otherwise(col("athlete_year_of_birth").cast("int"))
    )

    # Calculate athlete's age at the time of the event
    marathon_df = marathon_df.withColumn(
        "athlete_age_at_event",
        col("year_of_event") - col("athlete_year_of_birth")
    )

    # --------------------------------------------------------
    # Data Quality: Validate age is realistic
    # --------------------------------------------------------
    # Set unrealistic ages (< 5 or > 100) to null
    # Young children and very elderly participants are extremely rare in marathons
    
    marathon_df = marathon_df.withColumn(
        "athlete_age_at_event",
        when(
            (col("athlete_age_at_event") < 5)
            | (col("athlete_age_at_event") > 100),
            None
        ).otherwise(col("athlete_age_at_event"))
    )

    # Remove rows where birth year or calculated age is missing.
    # This keeps the Silver OBT clean for age-based analysis and dashboarding.
    marathon_df = marathon_df.filter(
        col("athlete_year_of_birth").isNotNull()
        & col("athlete_age_at_event").isNotNull()
    )

    # Standardize age category (e.g., "M40", "F35") to uppercase
    # Replace null/empty values with "unknown"
    marathon_df = marathon_df.withColumn(
        "athlete_age_category",
        when(
            col("athlete_age_category").isNull()
            | (length(trim(col("athlete_age_category"))) == 0),
            lit("unknown")
        ).otherwise(upper(trim(col("athlete_age_category"))))
    )

    # Standardize gender codes to uppercase and remove nulls
    marathon_df = (
        marathon_df
        .withColumn("athlete_gender", upper(trim(col("athlete_gender"))))
        .filter(col("athlete_gender").isNotNull())
    )

    # Ensure athlete_id is an integer
    marathon_df = marathon_df.withColumn(
        "athlete_id",
        col("athlete_id").cast("int")
    )

    # --------------------------------------------------------
    # Athlete cleaning: average speed
    # --------------------------------------------------------
    # The raw athlete_average_speed column contains scaling issues and inconsistent units.
    # Therefore, we recalculate the final speed from the cleaned event distance
    # and cleaned athlete performance columns.
    #
    # Speed calculation depends on event type:
    # 
    # For distance events (km or mi):
    #   speed (km/h) = distance in km / time in hours
    # 
    # For fixed-time events (hours):
    #   speed (km/h) = distance covered in km / event duration in hours

    marathon_df = marathon_df.withColumn(
        "athlete_average_speed_kmh",
        when(
            # Distance events in km:
            # speed = distance in km / performance hours
            (col("event_distance_unit") == "km")
            & col("athlete_performance_seconds").isNotNull(),
            col("event_distance_value") / (col("athlete_performance_seconds") / 3600)
        ).when(
            # Distance events in miles:
            # convert miles to km (1 mi = 1.60934 km), then divide by performance hours
            (col("event_distance_unit") == "mi")
            & col("athlete_performance_seconds").isNotNull(),
            (col("event_distance_value") * 1.60934) / (col("athlete_performance_seconds") / 3600)
        ).when(
            # Fixed-time events in hours where performance is in km:
            # speed = completed distance in km / event hours
            (col("event_distance_unit") == "h")
            & (col("athlete_performance_unit") == "km")
            & col("athlete_performance_distance").isNotNull(),
            col("athlete_performance_distance") / col("event_distance_value")
        ).when(
            # Fixed-time events where performance is in miles:
            # convert completed miles to km, then divide by event hours
            (col("event_distance_unit") == "h")
            & (col("athlete_performance_unit") == "mi")
            & col("athlete_performance_distance").isNotNull(),
            (col("athlete_performance_distance") * 1.60934) / col("event_distance_value")
        )
    )

    # Round speed to 3 decimal places for cleaner analysis and dashboarding
    marathon_df = marathon_df.withColumn(
    "athlete_average_speed_kmh",
        round(col("athlete_average_speed_kmh"), 3)
    )

    # --------------------------------------------------------
    # Data Quality: Remove unrealistic speeds
    # --------------------------------------------------------
    # Filter out records with:
    # - Missing speed (calculation failed)
    # - Zero or negative speed (impossible)
    # - Speed > 50 km/h (world record marathon pace is ~20 km/h, so 50 km/h is clearly an error)
    
    marathon_df = marathon_df.filter(
        col("athlete_average_speed_kmh").isNotNull()
        & (col("athlete_average_speed_kmh") > 0)
        & (col("athlete_average_speed_kmh") <= 50)
    )

    # --------------------------------------------------------
    # Country mapping enrichment
    # --------------------------------------------------------
    # Join with the country codes lookup table to enrich athlete records
    # with full country names and ISO3 codes for better reporting

    country_df = spark.sql(f"""
        SELECT *
        FROM {RAW_COUNTRY_CODES}
    """)

    # Clean the country lookup table
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

    # Perform inner join to enrich marathon data with country information
    # Inner join means we only keep records where we have a matching country code
    marathon_df = (
        marathon_df
        .join(
            country_df,
            marathon_df["athlete_country"] == country_df["athlete_country_code"],
            "inner"
        )
        .drop("athlete_country_code")  # Remove duplicate column after join
    )

    # --------------------------------------------------------
    # Remove exact duplicate result rows before creating IDs
    # --------------------------------------------------------
    # Some records may be exact duplicates across all core fields
    # Deduplication ensures each unique result appears only once
    
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
    # --------------------------------------------------------
    # Generate stable, deterministic IDs using SHA-256 hashing.
    # This ensures the same event or result always gets the same ID,
    # which is critical for incremental updates and dimensional modeling.
    #
    # Note: We use SHA-256 hashing instead of dense_rank() because this is
    # a streaming table, and window functions with dense_rank() are not
    # supported in streaming queries.

    marathon_df = (
        marathon_df
        .withColumn(
            "event_id",
            sha2(
                concat_ws(
                    "||",  # Delimiter to prevent hash collisions
                    col("event_name"),
                    col("event_distance_length")
                ),
                256  # SHA-256 produces a 64-character hex string
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
    # These intermediate columns were only needed for date parsing logic
    # and are not useful for downstream analysis
    
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
    # The returned DataFrame becomes the content of the streaming table
    # defined by the @dp.table decorator above

    return marathon_df
