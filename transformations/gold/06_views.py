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


@dp.materialized_view(
    name="marathos_catalog.gold.vw_distance_km_results",
    comment="Analytical view for kilometer-based marathon and ultramarathon results.",
)
def vw_distance_km_results():
    return spark.sql("""
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
        FROM marathos_catalog.gold.fact_results f
        INNER JOIN marathos_catalog.gold.dim_event e
            ON f.event_id = e.event_id
        INNER JOIN marathos_catalog.gold.dim_athlete a
            ON f.athlete_id = a.athlete_id
        INNER JOIN marathos_catalog.gold.dim_country c
            ON f.country_code_iso3 = c.country_code_iso3
        INNER JOIN marathos_catalog.gold.dim_date d
            ON f.date_id = d.date_id
        WHERE e.event_distance_type = 'distance_km'
    """)


@dp.materialized_view(
    name="marathos_catalog.gold.vw_distance_mi_results",
    comment="Analytical view for mile-based marathon and ultramarathon results.",
)
def vw_distance_mi_results():
    return spark.sql("""
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
        FROM marathos_catalog.gold.fact_results f
        INNER JOIN marathos_catalog.gold.dim_event e
            ON f.event_id = e.event_id
        INNER JOIN marathos_catalog.gold.dim_athlete a
            ON f.athlete_id = a.athlete_id
        INNER JOIN marathos_catalog.gold.dim_country c
            ON f.country_code_iso3 = c.country_code_iso3
        INNER JOIN marathos_catalog.gold.dim_date d
            ON f.date_id = d.date_id
        WHERE e.event_distance_type = 'distance_mi'
    """)


@dp.materialized_view(
    name="marathos_catalog.gold.vw_time_hour_results",
    comment="Analytical view for fixed-hour marathon and ultramarathon results.",
)
def vw_time_hour_results():
    return spark.sql("""
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
        FROM marathos_catalog.gold.fact_results f
        INNER JOIN marathos_catalog.gold.dim_event e
            ON f.event_id = e.event_id
        INNER JOIN marathos_catalog.gold.dim_athlete a
            ON f.athlete_id = a.athlete_id
        INNER JOIN marathos_catalog.gold.dim_country c
            ON f.country_code_iso3 = c.country_code_iso3
        INNER JOIN marathos_catalog.gold.dim_date d
            ON f.date_id = d.date_id
        WHERE e.event_distance_type = 'time_hours'
    """)