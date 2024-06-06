--Actors History SCD Table Batch Backfill Query (query_4)
--Write a "backfill" query that can populate the entire actors_history_scd table in a single query.

INSERT INTO saidaggupati.actors_history_scd
--LAGGED CTE
WITH LAGGED AS (
SELECT 
actor,
quality_class,
CASE WHEN is_active THEN 1 ELSE 0  END AS is_active,
LAG(quality_class,1) OVER (PARTITION BY actor ORDER BY current_year) AS last_year_quality_class,
CASE WHEN LAG(is_active,1) OVER (PARTITION BY actor ORDER BY current_year) THEN 1 ELSE 0 END AS is_active_last_year,
current_year
FROM saidaggupati.actors
),
-- STREAK CONCEPT IN ACTION - STREAK CTE
STREAKED AS (
SELECT 
*,
--rolling sum activated here to form the streak_identifier
SUM(CASE WHEN is_active <> is_active_last_year THEN 1 ELSE 0 END)
    OVER (PARTITION BY actor ORDER BY current_year) AS streak_identifier
FROM LAGGED
),
--BACKFILL CTE
BACKFILL_QUERY AS (
SELECT actor, 
       streak_identifier,
       quality_class,
       is_active, 
--Resolving this previous issue here: Unexpected parameters (integer) for function concat. Expected: concat(E, array(E)) E, concat(array(E)) E, concat(array(E), E) E, concat(char(x), char(y)), concat(varbinary), concat(varchar)
 DATE(CONCAT(CAST(MIN(current_year) AS VARCHAR), '-01-01')) AS start_date,
 DATE(CONCAT(CAST(MAX(current_year) AS VARCHAR), '-12-31')) AS end_date, 
--MAX(CURRENT_YEAR) gave 2021 output
 2021 AS Current_year
FROM STREAKED
--Resolving this issue: -- 'quality_class' must be an aggregate expression or appear in GROUP BY clause
GROUP BY actor, streak_identifier, quality_class, is_active
ORDER BY actor, start_date
)

SELECT actor,
       quality_class,
       CASE WHEN is_active = 1 THEN True ELSE False END as is_active,
-- specified in the prompt (table should be appropriately modeled as a Type 2 Slowly Changing Dimension Table (start_date and end_date))
       start_date,
       end_date,
--partition key - please refer to partitioning piece in the code.
       current_year
 FROM BACKFILL_QUERY
