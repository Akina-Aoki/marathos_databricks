RESULTS_STAR_JOIN = f"""
FROM {FACT_RESULTS} f
INNER JOIN {DIM_EVENT} e
    ON f.event_id = e.event_id
INNER JOIN {DIM_ATHLETE} a
    ON f.athlete_id = a.athlete_id
INNER JOIN {DIM_COUNTRY} c
    ON f.country_code_iso3 = c.country_code_iso3
INNER JOIN {DIM_DATE} d
    ON f.date_id = d.date_id
"""