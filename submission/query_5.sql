INSERT INTO emmaisemma.actors_history_scd 
WITH last_season_scd as (
        select *
        from emmaisemma.actors_history_scd
        where current_year = 2000
    ),
    current_season_scd as (
        select * from emmaisemma.actors
        where current_year = 2001
    ),
    combined as (
        select   
            COALESCE(ls.actor, ts.actor) as actor,
            COALESCE(ls.start_date, ts.current_year) as start_date,
            COALESCE(ls.end_date, ts.current_year) as end_date,
            CASE
                WHEN ls.is_active <> ts.is_active THEN 1
                WHEN ls.quality_class <> ts.quality_class THEN 1
                WHEN ls.is_active = ts.is_active and ls.quality_class = ts.quality_class THEN 0
            END as did_change,
            ls.is_active as is_active_last_season,
            ts.is_active as is_active_this_season,
            ls.quality_class as quality_class_last_season,
            ts.quality_class as quality_class_this_season,
            1925 as current_year
        FROM last_season_scd ls
            FULL OUTER JOIN current_season_scd ts ON ls.actor = ts.actor
            AND ls.end_date + 1 = ts.current_year
    ),
    changes AS (
        select actor,
            current_year,
            case when did_change = 0 then ARRAY [
              CAST(
                ROW(
                  is_active_last_season,
                  quality_class_last_season,
                  start_date,
                  end_date + 1
                ) AS ROW(
                  is_active BOOLEAN,
                  quality_class VARCHAR,
                  start_date INTEGER,
                  end_date INTEGER
                )
              )
        ]
                when did_change = 1 then ARRAY [
              CAST(
                ROW(
                  is_active_last_season,
                  quality_class_last_season,
                  start_date,
                  end_date
                ) AS ROW(
                  is_active BOOLEAN,
                  quality_class VARCHAR,
                  start_date INTEGER,
                  end_date INTEGER
                )
              ),
              CAST(
                ROW(
                  is_active_this_season,
                  quality_class_this_season,
                  current_year,
                  current_year
                ) AS ROW(
                  is_active BOOLEAN,
                  quality_class VARCHAR,
                  start_date INTEGER,
                  end_date INTEGER
                )
              )
        ]
                when did_change IS NULL then ARRAY [
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
              is_active BOOLEAN,
              quality_class VARCHAR,
              start_date INTEGER,
              end_date INTEGER
            )
          )
        ]
            END AS change_array
        FROM combined
    )
SELECT actor,
    ar.quality_class,
    ar.is_active,
    ar.start_date,
    ar.end_date,
    current_year
FROM changes
    CROSS JOIN UNNEST (change_array) AS ar