-- Define a Common Table Expression (CTE) named 'last_year'
WITH last_year AS (
  -- Select all columns from 'actors_history_scd' table where 'current_year' is 2009
  SELECT * 
  FROM ningde95.actors_history_scd
  WHERE current_year = 2009
),

-- Define a CTE named 'this_year'
this_year AS (
  -- Select all columns from 'actors' table where 'current_year' is 2010
  SELECT *
  FROM ningde95.actors
  WHERE current_year = 2010
),

-- Define a CTE named 'COMBINED' that combines data from 'last_year' and 'this_year'
COMBINED AS (
  SELECT 
    -- Coalesce to get the first non-null 'actor' from either 'last_year' or 'this_year'
    COALESCE(ls.actor, ts.actor) AS actor,
    -- Coalesce to get the first non-null 'quality_class' from either 'last_year' or 'this_year'
    COALESCE(ls.quality_class, ts.quality_class) AS quality_class,
    -- Coalesce to get the first non-null 'start_date' from either 'last_year' or 'this_year'
    COALESCE(ls.start_date, ts.current_year) AS start_date,
    -- Coalesce to get the first non-null 'end_date' from either 'last_year' or 'this_year'
    COALESCE(ls.end_date, ts.current_year) AS end_date,
    -- Determine if 'is_active' status has changed between years
    CASE 
      WHEN ls.is_active <> ts.is_active THEN 1 
      WHEN ls.is_active = ts.is_active THEN 0
    END AS did_change,
    -- Select 'is_active' status from 'last_year'
    ls.is_active AS last_year_is_active,
    -- Select 'is_active' status from 'this_year'
    ts.is_active AS this_year_is_active,
    -- Set a constant value for 'current_year'
    2010 AS current_year
  FROM 
    last_year ls
    FULL OUTER JOIN this_year ts ON ls.actor = ts.actor AND ls.end_date + 1 = ts.current_year
),

-- Define a CTE named 'changes' that processes changes in actor data
changes AS (
  SELECT 
    actor,
    -- Use a CASE statement to build an array of changes
    CASE 
      WHEN did_change = 0 THEN ARRAY[
        CAST(
          ROW (
            quality_class,
            last_year_is_active,
            start_date,
            end_date + 1
          ) AS ROW (
            quality_class VARCHAR,
            is_active BOOLEAN,
            start_date INTEGER,
            end_date INTEGER
          )
        )
      ]
      WHEN did_change = 1 THEN ARRAY[
        CAST(
          ROW (
            quality_class,
            last_year_is_active,
            start_date,
            end_date
          ) AS ROW (
            quality_class VARCHAR,
            is_active BOOLEAN,
            start_date INTEGER,
            end_date INTEGER
          )
        ),
        CAST(
          ROW (
            quality_class,
            this_year_is_active,
            current_year,
            current_year
          ) AS ROW (
            quality_class VARCHAR,
            is_active BOOLEAN,
            start_date INTEGER,
            end_date INTEGER
          )
        )
      ]
      WHEN did_change IS NULL THEN ARRAY[
        CAST(
          ROW (
            quality_class,
            COALESCE(last_year_is_active, this_year_is_active),
            start_date,
            end_date
          ) AS ROW (
            quality_class VARCHAR,
            is_active BOOLEAN,
            start_date INTEGER,
            end_date INTEGER
          )
        )
      ]
    END AS change_array,
    current_year
  FROM 
    COMBINED
)

-- Select final result set by unnesting the 'change_array'
SELECT
  actor,
  arr.quality_class,
  arr.is_active,
  arr.start_date,
  arr.end_date,
  current_year
FROM 
  changes
  CROSS JOIN UNNEST(change_array) AS arr
