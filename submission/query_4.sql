--Query_4: Write a "backfill" query that can populate the entire 'actors_history_scd' table into a single query

INSERT INTO
	dswills94.actors_history_scd
	--1 load bacfill query, use a CTE
WITH lagged AS (
	--We create CTE lagged to peek at prior years data from current year
	SELECT
		actor,
		actor_id,
		quality_class,
		LAG(quality_class, 1) OVER (PARTITION BY actor_id ORDER BY current_year) AS quality_class_last_year,
		--Peak back one year to see prior quality_class
		CASE
			WHEN is_active THEN 1
			ELSE 0
		END AS is_active,
		CASE
			WHEN LAG(is_active, 1) OVER (PARTITION BY actor_id ORDER BY current_year) THEN 1
			ELSE 0
		END AS is_active_last_year,
		--Peak back one year to see prior activity
		current_year
	FROM
		dswills94.actors
	WHERE
		current_year <= 1918
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
)
SELECT 
	actor,
	actor_id,
	quality_class,
	MAX(is_active) = 1 AS is_active,
	-- To make this a boolean
	MIN(current_year) AS start_date,
	MAX(current_year) AS end_date,
	1918 AS current_year
FROM
	streaked
GROUP BY
	actor,
	actor_id,
	quality_class,
	streak_identifier
