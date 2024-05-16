--Ideally we would parameterize the current_year and would delete all records for the current_year prior to running so that the process is idempotent
--Once data quality checks have been run successfully, previous partitions of actors_history_scd can be deleted
INSERT INTO actors_history_scd
WITH last_year_scd AS 
(
	SELECT *
	FROM actors_history_scd
	WHERE current_year = 1919
), current_year_table AS 
(
	SELECT *
	FROM actors
	WHERE current_year = 1920
), current_year_value AS
(
  SELECT MAX(current_year) AS max_current_year
  FROM current_year_table
), combined AS 
(
	SELECT COALESCE(lys.actor,cyt.actor) AS actor,
	  COALESCE(lys.actor_id, cyt.actor_id) AS actor_id,
	  --Check quality_class and is_active to detect a change. A NULL for did_change indicates a previous year which will be carried forward with no changes.
	  CASE WHEN lys.quality_class = cyt.quality_class AND lys.is_active = cyt.is_active THEN 0
	    WHEN lys.quality_class <> cyt.quality_class OR lys.is_active <> cyt.is_active THEN 1 END AS did_change,
	  cyv.max_current_year AS current_year,
	  COALESCE(lys.start_date, CAST(CAST(cyt.current_year AS VARCHAR) || '-01-01' AS DATE)) AS start_date,
	  COALESCE(lys.end_date, CAST(CAST(cyt.current_year AS VARCHAR) || '-12-31' AS DATE)) AS end_date,
	  lys.quality_class AS last_quality_class,
	  cyt.quality_class AS current_quality_class,
	  lys.is_active AS last_is_active,
	  cyt.is_active AS current_is_active
	FROM last_year_scd lys
		FULL OUTER JOIN current_year_table cyt ON lys.actor_id = cyt.actor_id
			AND lys.end_date + INTERVAL '1' YEAR = CAST(CAST(cyt.current_year AS VARCHAR) || '-12-31' AS DATE)
		CROSS JOIN current_year_value cyv
), changes AS 
(
  SELECT actor,
    actor_id,
    current_year,
	--when did_change = 0 - take the values from last year's row, but update the end_date to the current_year
	--when did_change = 1 - Pull last year's data forward and add a new row for the current year
	--when did_change IS NULL - This is a previous year. Add the row to an array to carry it forward
    CASE WHEN did_change = 0 THEN ARRAY[CAST(ROW(last_quality_class, last_is_active, start_date, CAST(CAST(current_year AS VARCHAR) || '-12-31' AS DATE)) AS ROW(quality_class VARCHAR, is_active BOOLEAN, start_date DATE, end_date DATE))]
      WHEN did_change = 1 THEN ARRAY[CAST(ROW(last_quality_class, last_is_active, start_date, end_date) AS ROW(quality_class VARCHAR, is_active BOOLEAN, start_date DATE, end_date DATE)), CAST(ROW(current_quality_class, current_is_active, CAST(CAST(current_year AS VARCHAR) || '-01-01' AS DATE), CAST(CAST(current_year AS VARCHAR) || '-12-31' AS DATE)) AS ROW(quality_class VARCHAR, is_active BOOLEAN, start_date DATE, end_date DATE))]
      WHEN did_change IS NULL THEN ARRAY[CAST(ROW(COALESCE(last_quality_class, current_quality_class), COALESCE(last_is_active, current_is_active), start_date, end_date) AS ROW(quality_class VARCHAR, is_active BOOLEAN, start_date DATE, end_date DATE))] END AS change_array
  FROM combined
)
SELECT c.actor,
  c.actor_id,
  arr.quality_class,
  arr.is_active,
  arr.start_date,
  arr.end_date,
  c.current_year
FROM changes c
  CROSS JOIN UNNEST(c.change_array) arr
