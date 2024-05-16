INSERT INTO actors_history_scd 
with last_year_scd as(
select * from actors_history_scd
where current_year =2020

)
--get this year records
, this_year_scd as(
select * from actors
where current_year = 2021
)
--combining the records from last year and this year and evaluating different case statements to see if there's a change in is_active and quality class from last year to this year
, combined as (
select 
  COALESCE(LY.actor,TY.actor) actor,
  COALESCE(LY.start_date,TY.current_year) start_date,
  COALESCE(LY.end_date,TY.current_year) end_date,
  CASE
      WHEN LY.is_active != TY.is_active
        OR LY.quality_class != TY.quality_class THEN 1
      WHEN LY.is_active = TY.is_active
        AND LY.quality_class = TY.quality_class THEN 0
  END AS did_change,
  LY.is_active AS is_active_last_year,
  TY.is_active AS is_active_current_year,
  LY.quality_class AS quality_class_last_year,
  TY.quality_class AS quality_class_current_year,

  2021 as current_year
FROM  last_year_scd as LY
FULL OUTER JOIN this_year_scd as TY
  ON LY.actor = TY.actor
  AND LY.end_date + 1 = TY.current_year
)

--creating a row of Array type based on did_change.
, changes as(
select 
       actor,
       current_year,
       CASE 
       WHEN did_change = 0 THEN 
       ARRAY[
         CAST( ROW(
                  quality_class_last_year,
                  is_active_last_year,
                  start_date,
                  end_date + 1
                 )
          AS ROW( 
                  quality_class VARCHAR,
                  is_active BOOLEAN,
                  start_date INTEGER,
                  end_date INTEGER
                )
            )
        ]
        WHEN did_change = 1 THEN 
       ARRAY[
         CAST( ROW(
                  quality_class_last_year,
                  is_active_last_year,
                  start_date,
                  end_date + 1
                 )
          AS ROW( 
                  quality_class VARCHAR,
                  is_active BOOLEAN,
                  start_date INTEGER,
                  end_date INTEGER
                )
            ),
        CAST( ROW(
                  quality_class_current_year,
                  is_active_current_year,
                  current_year,
                  current_year
                 )
          AS ROW( 
                  quality_class VARCHAR,
                  is_active BOOLEAN,
                  start_date INTEGER,
                  end_date INTEGER
                )
            )
        ]
        WHEN did_change IS NULL THEN 
        ARRAY[
          CAST(
            ROW(
          COALESCE(quality_class_last_year, 
                 quality_class_current_year),
          COALESCE(is_active_last_year, 
                  is_active_current_year),
          start_date,
          end_date
               ) 
          AS ROW(
                quality_class VARCHAR,
                is_active BOOLEAN,
                start_date INTEGER,
                end_date INTEGER
                      )
            )
          ]
            END AS change_array 
from combined 
)
--unnest the change_array to insert into the scd table
SELECT
    actor,
    c_arr.quality_class,
    c_arr.is_active,
    c_arr.start_date,
    c_arr.end_date,
    current_year
FROM
    changes
    CROSS JOIN UNNEST (change_array) as c_arr
