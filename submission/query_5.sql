INSERT INTO chinmay_hebbal.actors_history_scd
WITH last_year_actors_scd AS (
    SELECT *
    FROM chinmay_hebbal.actors_history_scd
    WHERE current_year = 2010
),
this_year_actors AS (
    SELECT *
    FROM chinmay_hebbal.actors
    WHERE current_year = 2011
),
combined_scd AS (
    SELECT COALESCE(ls.actor, ts.actor) AS actor,
        COALESCE(ls.actor_id, ts.actor_id) AS actor_id,
        COALESCE(ls.start_date, ts.current_year) AS start_date,
        COALESCE(ls.end_date, ts.current_year) AS end_date,
        CASE
            WHEN ls.is_active <> ts.is_active
            OR ls.quality_class <> ts.quality_class
            OR ls.average_rating <> ts.average_rating THEN 1
            WHEN ls.is_active = ts.is_active
            OR ls.quality_class = ts.quality_class
            OR ls.average_rating = ts.average_rating THEN 0
        END AS record_changed,
        ls.is_active AS is_active_last_year,
        ts.is_active AS is_active_this_year,
        ls.average_rating AS average_rating_last_year,
        ts.average_rating AS average_rating_this_year,
        ls.quality_class AS quality_class_last_year,
        ts.quality_class AS quality_class_this_year,
        2011 AS current_year
    FROM last_year_actors_scd ls
    FULL OUTER JOIN this_year_actors ts ON ls.actor_id = ts.actor_id
    AND ls.end_date + 1 = ts.current_year
),
changed AS (
    SELECT actor,
        actor_id,
        current_year,
        CASE
            WHEN record_changed = 0 THEN ARRAY [ROW(
            is_active_last_year, average_rating_last_year, quality_class_last_year, start_date, end_date + 1)]
            WHEN record_changed = 1 THEN ARRAY [ROW(
            is_active_last_year, average_rating_last_year, quality_class_last_year, start_date, end_date),
            ROW(
            is_active_this_year, average_rating_this_year, quality_class_this_year, current_year, current_year)]
            WHEN record_changed IS NULL THEN ARRAY [ROW(
            COALESCE(is_active_last_year, is_active_this_year),
            COALESCE(average_rating_last_year, average_rating_this_year),
            COALESCE(quality_class_last_year, quality_class_this_year),
            start_date, end_date)]
        END AS record_changed_array
    FROM combined_scd
)
SELECT actor,
    actor_id,
    arr.is_active,
    arr.average_rating,
    arr.quality_class,
    arr.start_date,
    arr.end_date,
    current_year
FROM changed
    CROSS JOIN UNNEST(record_changed_array) AS arr (
        is_active,
        average_rating,
        quality_class,
        start_date,
        end_date
    )
```
