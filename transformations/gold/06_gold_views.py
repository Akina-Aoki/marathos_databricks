"""
Gold Layer - Analytical Views

This module creates analytical Gold views for the different marathon event types.
The views are built from the Gold star schema tables and are designed for
dashboarding and analysis.

The marathon result types are:
- distance_km: events measured by kilometers, where athlete performance is time
- distance_mi: events measured by miles, where athlete performance is time
- time_hours: fixed-hour events, where athlete performance is distance completed
"""

from pyspark import pipelines as dp

from utils.table_names import (
    DIM_ATHLETE,
    DIM_COUNTRY,
    DIM_DATE,
    DIM_EVENT,
    FACT_RESULTS,
    VW_COUNTRY_SEASONAL_TRENDS,
    VW_DISTANCE_KM_RESULTS,
    VW_DISTANCE_MI_RESULTS,
    VW_GLOBAL_LEADERBOARD,
    VW_RUNNER_DEMOGRAPHICS,
    VW_SEASONAL_RACE_TRENDS,
    VW_TIME_HOUR_RESULTS,
)

@dp.materialized_view(
    name=VW_DISTANCE_KM_RESULTS,
    comment="Analytical view for kilometer-based marathon and ultramarathon results.",
)
def vw_distance_km_results():
    return spark.sql(f"""
        SELECT
            f.result_id,
            d.event_start_date,
            d.year,
            d.month,
            d.month_name,
            d.day,
            e.event_name,
            e.event_distance_length,
            e.event_distance_value,
            e.event_distance_unit,
            a.athlete_id,
            a.athlete_gender,
            a.athlete_year_of_birth,
            a.athlete_age_category,
            c.athlete_country,
            c.country_name,
            f.event_number_of_finishers,
            f.athlete_performance,
            f.athlete_performance_seconds,
            f.athlete_average_speed_kmh,
            f.athlete_age_at_event
        FROM {FACT_RESULTS} f
        INNER JOIN {DIM_EVENT} e
            ON f.event_id = e.event_id
        INNER JOIN {DIM_ATHLETE} a
            ON f.athlete_id = a.athlete_id
        INNER JOIN {DIM_COUNTRY} c
            ON f.country_code_iso3 = c.country_code_iso3
        INNER JOIN {DIM_DATE} d
            ON f.date_id = d.date_id
        WHERE e.event_distance_type = 'distance_km'
    """)


@dp.materialized_view(
    name=VW_DISTANCE_MI_RESULTS,
    comment="Analytical view for mile-based marathon and ultramarathon results.",
)
def vw_distance_mi_results():
    return spark.sql(f"""
        SELECT
            f.result_id,
            d.event_start_date,
            d.year,
            d.month,
            d.month_name,
            d.day,
            e.event_name,
            e.event_distance_length,
            e.event_distance_value,
            e.event_distance_unit,
            a.athlete_id,
            a.athlete_gender,
            a.athlete_year_of_birth,
            a.athlete_age_category,
            c.athlete_country,
            c.country_name,
            f.event_number_of_finishers,
            f.athlete_performance,
            f.athlete_performance_seconds,
            f.athlete_average_speed_kmh,
            f.athlete_age_at_event
        FROM {FACT_RESULTS} f
        INNER JOIN {DIM_EVENT} e
            ON f.event_id = e.event_id
        INNER JOIN {DIM_ATHLETE} a
            ON f.athlete_id = a.athlete_id
        INNER JOIN {DIM_COUNTRY} c
            ON f.country_code_iso3 = c.country_code_iso3
        INNER JOIN {DIM_DATE} d
            ON f.date_id = d.date_id
        WHERE e.event_distance_type = 'distance_mi'
    """)


@dp.materialized_view(
    name=VW_TIME_HOUR_RESULTS,
    comment="Analytical view for fixed-hour marathon and ultramarathon results.",
)
def vw_time_hour_results():
    return spark.sql(f"""
        SELECT
            f.result_id,
            d.event_start_date,
            d.year,
            d.month,
            d.month_name,
            d.day,
            e.event_name,
            e.event_distance_length,
            e.event_distance_value,
            e.event_distance_unit,
            a.athlete_id,
            a.athlete_gender,
            a.athlete_year_of_birth,
            a.athlete_age_category,
            c.athlete_country,
            c.country_name,
            f.event_number_of_finishers,
            f.athlete_performance,
            f.athlete_performance_distance,
            f.athlete_average_speed_kmh,
            f.athlete_age_at_event
        FROM {FACT_RESULTS} f
        INNER JOIN {DIM_EVENT} e
            ON f.event_id = e.event_id
        INNER JOIN {DIM_ATHLETE} a
            ON f.athlete_id = a.athlete_id
        INNER JOIN {DIM_COUNTRY} c
            ON f.country_code_iso3 = c.country_code_iso3
        INNER JOIN {DIM_DATE} d
            ON f.date_id = d.date_id
        WHERE e.event_distance_type = 'time_hours'
    """)


    # ------------------------------------------------------------
# Business view: Global leaderboard
# ------------------------------------------------------------

@dp.materialized_view(
    name=VW_GLOBAL_LEADERBOARD,
    comment="Country-level participation and performance leaderboard."
)
def vw_global_leaderboard():
    return spark.sql(f"""
        SELECT
            c.country_name,
            c.country_code_iso3 AS country_code,

            COUNT(*) AS race_result_records,
            COUNT(DISTINCT f.athlete_id) AS unique_athletes,
            COUNT(DISTINCT f.event_id) AS events_participated,

            ROUND(AVG(f.athlete_average_speed_kmh), 3) AS average_speed_kmh,
            ROUND(AVG(f.athlete_age_at_event), 1) AS average_athlete_age

        FROM {FACT_RESULTS} f
        INNER JOIN {DIM_COUNTRY} c
            ON f.country_code_iso3 = c.country_code_iso3

        GROUP BY
            c.country_name,
            c.country_code_iso3
    """)

# ------------------------------------------------------------
# Business view: Seasonal race trends
# ------------------------------------------------------------

@dp.materialized_view(
    name=VW_SEASONAL_RACE_TRENDS,
    comment="Monthly marathon and ultramarathon activity trends."
)
def vw_seasonal_race_trends():
    return spark.sql(f"""
        SELECT
            d.year AS race_year,
            d.month_name AS race_month,

            COUNT(*) AS athlete_entries,
            COUNT(DISTINCT f.athlete_id) AS unique_athletes,
            COUNT(DISTINCT f.event_id) AS events_held,

            ROUND(AVG(f.athlete_average_speed_kmh), 3) AS average_speed_kmh,
            ROUND(AVG(f.athlete_age_at_event), 1) AS average_athlete_age

        FROM {FACT_RESULTS} f
        INNER JOIN {DIM_DATE} d
            ON f.date_id = d.date_id

        GROUP BY
            d.year,
            d.month,
            d.month_name
    """)

    # ------------------------------------------------------------
# Business view: Runner demographics
# ------------------------------------------------------------

@dp.materialized_view(
    name=VW_RUNNER_DEMOGRAPHICS,
    comment="Runner participation and performance by gender and age group."
)
def vw_runner_demographics():
    return spark.sql(f"""
        WITH demographic_base AS (
            SELECT
                CASE
                    WHEN a.athlete_gender = 'M' THEN 'Male'
                    WHEN a.athlete_gender = 'F' THEN 'Female'
                    ELSE 'Other / Unknown'
                END AS gender,

                CASE
                    WHEN a.athlete_age_category LIKE '%U%' THEN CONCAT('Under ', regexp_extract(a.athlete_age_category, '[0-9]+', 0))
                    WHEN a.athlete_age_category LIKE 'M%' THEN CONCAT(regexp_extract(a.athlete_age_category, '[0-9]+', 0), '+')
                    WHEN a.athlete_age_category LIKE 'W%' THEN CONCAT(regexp_extract(a.athlete_age_category, '[0-9]+', 0), '+')
                    ELSE a.athlete_age_category
                END AS age_group,

                CASE
                    WHEN a.athlete_age_category LIKE '%U%' THEN CAST(regexp_extract(a.athlete_age_category, '[0-9]+', 0) AS INT) - 1
                    WHEN a.athlete_age_category LIKE 'M%' THEN CAST(regexp_extract(a.athlete_age_category, '[0-9]+', 0) AS INT)
                    WHEN a.athlete_age_category LIKE 'W%' THEN CAST(regexp_extract(a.athlete_age_category, '[0-9]+', 0) AS INT)
                    ELSE 999
                END AS age_group_sort,

                f.athlete_id,
                f.athlete_average_speed_kmh,
                f.athlete_age_at_event

            FROM {FACT_RESULTS} f
            INNER JOIN {DIM_ATHLETE} a
                ON f.athlete_id = a.athlete_id

            WHERE a.athlete_age_category IS NOT NULL
              AND LOWER(a.athlete_age_category) <> 'unknown'
        ),

        demographic_summary AS (
            SELECT
                gender,
                age_group,
                age_group_sort,
                COUNT(*) AS athlete_entries,
                COUNT(DISTINCT athlete_id) AS unique_athletes,
                ROUND(AVG(athlete_average_speed_kmh), 3) AS average_speed_kmh,
                ROUND(AVG(athlete_age_at_event), 1) AS average_athlete_age
            FROM demographic_base
            GROUP BY
                gender,
                age_group,
                age_group_sort
        )

        SELECT
            gender,
            age_group,
            athlete_entries,
            unique_athletes,
            average_speed_kmh,
            average_athlete_age

        FROM demographic_summary
    """)


# ------------------------------------------------------------
# Business view: Country seasonal trends
# ------------------------------------------------------------

@dp.materialized_view(
    name=VW_COUNTRY_SEASONAL_TRENDS,
    comment="Monthly race activity trends by athlete country."
)
def vw_country_seasonal_trends():
    return spark.sql(f"""
        SELECT
            d.year AS race_year,
            d.month_name AS race_month,
            c.country_name,

            COUNT(*) AS athlete_entries,
            COUNT(DISTINCT f.athlete_id) AS unique_athletes,
            COUNT(DISTINCT f.event_id) AS events_held,

            ROUND(AVG(f.athlete_average_speed_kmh), 3) AS average_speed_kmh,
            ROUND(AVG(f.athlete_age_at_event), 1) AS average_athlete_age

        FROM {FACT_RESULTS} f
        INNER JOIN {DIM_DATE} d
            ON f.date_id = d.date_id
        INNER JOIN {DIM_COUNTRY} c
            ON f.country_code_iso3 = c.country_code_iso3
            

        GROUP BY
            d.year,
            d.month,
            d.month_name,
            c.country_name
    """)

