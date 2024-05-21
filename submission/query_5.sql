INSERT INTO actors_history_scd WITH last_season_scd AS (
    -- previous SCD 'state' - assumed to be populated by previous pipeline run
        SELECT *
        FROM actors_history_scd
        WHERE current_year = 1924
    ),
    current_season_scd AS (
    -- current year SCD data is calculated based on source table containing the dimensions
        SELECT *
        FROM actors
        WHERE current_year = 1925
    ),
    combined AS (
        SELECT COALESCE(ls.actor, cs.actor) AS actor,
            COALESCE(ls.start_date, cs.current_year) AS start_date,
            COALESCE(ls.end_date, cs.current_year) AS end_date,
            CASE
                WHEN ls.is_active <> cs.is_active THEN 1
                WHEN ls.quality_class <> cs.quality_class THEN 1
                WHEN ls.is_active = cs.is_active
                AND ls.quality_class = cs.quality_class THEN 0
            END AS did_change,
            -- true if even one of 2 tracked attributes changes
            ls.is_active AS is_active_last_season,
            cs.is_active AS is_active_this_season,
            ls.quality_class AS quality_class_last_season,
            cs.quality_class AS quality_class_this_season,
            1925 as current_year
        FROM last_season_scd ls
            FULL OUTER JOIN current_season_scd cs ON ls.actor = cs.actor
            AND ls.end_date + 1 = cs.current_year
            -- full outer join to track both new actors and actors going inactive
    ),
    changes AS (
        SELECT actor,
            current_year,
            CASE
                WHEN did_change = 0 THEN ARRAY [
                    -- no changes - just extend SCD by one year
          CAST(
            ROW(
              is_active_last_season,
              quality_class_last_season,
              start_date,
              end_date + 1
            ) AS ROW(
              is_active boolean,
              quality_class varchar,
              start_date integer,
              end_date integer
            )
          )
        ]
                WHEN did_change = 1 THEN ARRAY [
                    -- one of 2 attributes changed - keep last year SCD data and create a new row to represent new 'streak'
          CAST(
            ROW(
              is_active_last_season,
              quality_class_last_season,
              start_date,
              end_date
            ) AS ROW(
              is_active boolean,
              quality_class varchar,
              start_date integer,
              end_date integer
            )
          ),
          CAST(
            ROW(
              is_active_this_season,
              quality_class_this_season,
              current_year,
              current_year
            ) AS ROW(
              is_active boolean,
              quality_class varchar,
              start_date integer,
              end_date integer
            )
          )
        ]
                WHEN did_change IS NULL THEN ARRAY [
                    -- special case - new actor or actor whose data is not in current_year
          CAST(
            ROW(
              COALESCE(is_active_last_season, is_active_this_season),
              COALESCE(
                quality_class_last_season,
                quality_class_this_season
              ),
              start_date,
              end_date
            ) AS ROW(
              is_active boolean,
              quality_class varchar,
              start_date integer,
              end_date integer
            )
          )
        ]
            END AS change_array
        FROM combined
    )
SELECT actor,
    arr.quality_class,
    arr.is_active,
    arr.start_date,
    arr.end_date,
    current_year
FROM changes
    CROSS JOIN UNNEST (change_array) AS arr
    -- explode array into multiple rows to create SCD 'streaks'