-- Define a Common Table Expression (CTE) named 'lagged'
WITH lagged AS (
  -- Select specific columns and calculate lagged values from 'ningde95.actors' table
  SELECT 
    actor,  -- Select the 'actor' column
    is_active,  -- Select the 'is_active' column
    quality_class,  -- Select the 'quality_class' column
    -- Calculate the lagged value of 'is_active' by 1 row within each actor's partition ordered by 'current_year'
    LAG(is_active, 1) OVER (PARTITION BY actor ORDER BY current_year) AS is_active_last_year,
    current_year  -- Select the 'current_year' column
  FROM 
    ningde95.actors
),

-- Define a CTE named 'streaked' that builds upon 'lagged'
streaked AS (
  -- Select specific columns and calculate streak identifiers
  SELECT 
    actor,  -- Select the 'actor' column
    is_active,  -- Select the 'is_active' column
    current_year,  -- Select the 'current_year' column
    quality_class,  -- Select the 'quality_class' column
    -- Calculate streak identifier based on changes in 'is_active' status
    SUM(CASE WHEN is_active <> is_active_last_year THEN 1 ELSE 0 END) OVER (PARTITION BY actor ORDER BY current_year) AS streak_identifier
  FROM 
    lagged
)

-- Select the final result set with various aggregations
SELECT 
  actor,  -- Select the 'actor' column
  MAX(quality_class) AS quality_class,  -- Get the maximum 'quality_class' for each group
  MAX(is_active) AS is_active,  -- Get the maximum 'is_active' status for each group
  MIN(current_year) AS start_date,  -- Get the minimum 'current_year' as the start date for each group
  MAX(current_year) AS end_date,  -- Get the maximum 'current_year' as the end date for each group
  2009 AS current_year  -- Set a constant value '2009' as 'current_year'
FROM 
  streaked
-- Group by 'actor' and 'streak_identifier' to aggregate the results
GROUP BY 
  actor, 
  streak_identifier
