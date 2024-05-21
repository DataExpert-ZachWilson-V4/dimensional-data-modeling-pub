INSERT INTO
	dswills94.actors_history_scd
	--1 load bacfill query, use a CTE
WITH lagged AS (
--We create CTE lagged to peek at prior years data from current year
SELECT
	actor,
	--actor name
	actor_id,
	--id of actor
	quality_class,
	--qualifier of film quality
	LAG(quality_class, 1) OVER (PARTITION BY actor_id ORDER BY current_year) AS quality_class_last_year,
	--Peak back one year to see prior quality_class
		CASE
			WHEN is_active THEN 1
		ELSE 0
	    END AS is_active,
	--active flag
		CASE
			WHEN LAG(is_active, 1) OVER (PARTITION BY actor_id ORDER BY current_year) THEN 1
		ELSE 0
	    END AS is_active_last_year,
	--Peak back one year to see prior activity
	current_year
	--current year
FROM
		dswills94.actors
	--actors data table
WHERE
		current_year <= 1918
	--we are backfilling from current year
),
	streaked AS (
--We are trying to find the streaks in data
SELECT
		*,
		CASE
			WHEN quality_class <> quality_class_last_year THEN 1
		    WHEN is_active <> is_active_last_year THEN 1
		ELSE 0
	END AS did_change,
	--We need to track quality_class and activity changes
	SUM(CASE
			WHEN quality_class <> quality_class_last_year THEN 1
			WHEN is_active <> is_active_last_year THEN 1
			ELSE 0 END) OVER(PARTITION BY actor_id ORDER BY current_year) AS streak_identifier
	---We need a rolling sum over actor_id to cumulate activity and quality_class changes
FROM
		lagged
	--CTE lagged to peek at prior year data
)
SELECT 
	actor,
	--actor name
	actor_id,
	--id of actor
	quality_class,
	--qualifier of film quality
	MAX(is_active) = 1 AS is_active,
	-- To make this a boolean
	MIN(current_year) AS start_date,
	--to see oldest date in backfill
	MAX(current_year) AS end_date,
	--to see the latest date in backfill
	1918 AS current_year
FROM
	streaked
GROUP BY
	--group by to see data per actor, actor_id, quality_class, streak_identifier
	actor,
	actor_id,
	quality_class,
	streak_identifier
