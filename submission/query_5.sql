-- The following query inserts changes from 2010 into the actors_history_scd table which,
-- at this point, has date only until 2009.
INSERT INTO actors_history_scd
WITH last_year_scd AS (
    SELECT * FROM actors_history_scd
    WHERE current_year = 2009
),
this_year_scd AS (
    SELECT * FROM actors
    WHERE current_year = 2010
),
combined AS (
    SELECT
        COALESCE(ly.actor_id, ty.actor_id) AS actor_id,
        COALESCE(ly.actor, ty.actor) AS actor,
        COALESCE(ly.start_date, ty.current_year) AS start_date,
        COALESCE(ly.end_date, ly.current_year) AS end_date,
        CASE
            WHEN ly.is_active <> ty.is_active THEN TRUE
            WHEN ly.is_active = ty.is_active THEN FALSE
        END AS is_active_did_change,
        CASE
            WHEN ly.quality_class <> ty.quality_class THEN TRUE
            WHEN ly.quality_class = ty.quality_class THEN FALSE
        END AS quality_class_did_change,
        ly.is_active AS is_active_last_year,
        ty.is_active AS is_active_this_year,
        ty.quality_class AS quality_class_this_year,
        ly.quality_class AS quality_class_last_year,
        2010 AS current_year
    FROM
        last_year_scd ly
        FULL OUTER JOIN this_year_scd ty ON ly.actor_id = ty.actor_id
        AND ly.end_date + 1 = ty.current_year
),
changes AS (
    SELECT
        actor_id,
        actor,
        current_year,
        CASE
            -- No changes
            WHEN NOT is_active_did_change AND NOT quality_class_did_change THEN ARRAY[
                CAST(
                    ROW(is_active_last_year, quality_class_last_year, start_date, end_date + 1)
                    AS ROW(is_active BOOLEAN, quality_class VARCHAR, start_date INTEGER, end_date INTEGER)
                )
            ]
            -- Is active changed
            WHEN is_active_did_change AND NOT quality_class_did_change THEN ARRAY[
                CAST(
                    ROW(is_active_last_year, quality_class_last_year, start_date, end_date)
                    AS ROW(is_active BOOLEAN, quality_class VARCHAR, start_date INTEGER, end_date INTEGER)
                ),
                CAST(
                    ROW(is_active_this_year, quality_class_last_year, current_year, current_year)
                    AS ROW(is_active BOOLEAN, quality_class VARCHAR, start_date INTEGER, end_date INTEGER)
                )
            ]
            -- Quality class changed
            WHEN NOT is_active_did_change AND quality_class_did_change THEN ARRAY[
                CAST(
                    ROW(is_active_last_year, quality_class_last_year, start_date, end_date)
                    AS ROW(is_active BOOLEAN, quality_class VARCHAR, start_date INTEGER, end_date INTEGER)
                ),
                CAST(
                    ROW(is_active_last_year, quality_class_this_year, current_year, current_year)
                    AS ROW(is_active BOOLEAN, quality_class VARCHAR, start_date INTEGER, end_date INTEGER)
                )
            ]
            -- Both changed
            WHEN is_active_did_change AND quality_class_did_change THEN ARRAY[
                CAST(
                    ROW(is_active_last_year, quality_class_last_year, start_date, end_date)
                    AS ROW(is_active BOOLEAN, quality_class VARCHAR, start_date INTEGER, end_date INTEGER)
                ),
                CAST(
                    ROW(is_active_this_year, quality_class_this_year, current_year, current_year)
                    AS ROW(is_active BOOLEAN, quality_class VARCHAR, start_date INTEGER, end_date INTEGER)
                )
            ]
            -- Did change is null (new record)
            WHEN is_active_did_change IS NULL AND quality_class_did_change IS NULL THEN ARRAY[
                CAST(
                    ROW(
                        COALESCE(is_active_last_year, is_active_this_year),
                        COALESCE(quality_class_last_year, quality_class_this_year),
                        start_date,
                        end_date
                    )
                    AS ROW(is_active BOOLEAN, quality_class VARCHAR, start_date INTEGER, end_date INTEGER)
                )
            ]
        END AS change_array
    FROM
        combined
)
SELECT
    actor_id,
    actor,
    quality_class,
    is_active,
    start_date,
    end_date,
    current_year
FROM changes 
CROSS JOIN UNNEST(change_array) AS arr