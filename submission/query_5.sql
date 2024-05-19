--Query_5 Write an "incremental" query that can populate a single year's worth of the actors_history_scd table by combining the previous year's SCD data with the new incoming data from the actors table for this year

INSERT INTO dswills94.actors_history_scd
WITH last_year_scd AS (
SELECT
	*
FROM
	dswills94.actors_history_scd
WHERE
	current_year = 1917
),
 curent_year_scd AS (
SELECT
	*
FROM
	dswills94.actors
WHERE
	current_year = 1918
),
combined AS (
--We to match on the records that have potential to change
SELECT 
	COALESCE(ly.actor, cy.actor) AS actor,
	COALESCE(ly.actor_id, cy.actor_id) AS actor_id,
	COALESCE(ly.start_date, cy.current_year) AS start_date,
	COALESCE(ly.end_date, cy.current_year) AS end_date,
	CASE
		WHEN ly.is_active <> cy.is_active THEN 1
		WHEN ly.is_active = cy.is_active THEN 0
	END AS did_change,
	--We need to add new record when activity changes, extend old record when it doesn't
	CASE
		WHEN ly.quality_class <> cy.quality_class THEN 1
		WHEN ly.quality_class = cy.quality_class THEN 0
	END AS qc_did_change,
	--We need to add new record when quality_class changes, extend old record when it doesn't
	ly.is_active AS is_active_last_year,
	cy.is_active AS is_active_this_year,
	ly.quality_class AS quality_class_last_year,
	cy.quality_class AS quality_class_this_year,
	1918 AS current_year
FROM
	last_year_scd ly
FULL OUTER JOIN curent_year_scd cy
	-- capturing brand new actors
ON
	ly.actor_id = cy.actor_id
	AND ly.end_date + 1 = cy.current_year
),
changes AS (
SELECT
	actor,
	actor_id,
	current_year,
	CASE
		WHEN did_change = 0
		THEN ARRAY[CAST(ROW(is_active_last_year,
		quality_class_last_year,
		start_date,
		end_date + 1) AS ROW(is_active BOOLEAN,
		quality_class VARCHAR,
		start_date INTEGER,
		end_date INTEGER))]
		--When activity record doesn't change, but we want to extend old record with updated interval
		WHEN did_change = 1
		THEN ARRAY[
		CAST(ROW(is_active_last_year,
		quality_class_last_year,
		start_date,
		end_date) AS ROW(is_active BOOLEAN,
		quality_class VARCHAR,
		start_date INTEGER,
		end_date INTEGER)),
		CAST(ROW(is_active_this_year,
		quality_class_this_year,
		current_year,
		current_year) AS ROW(is_active BOOLEAN,
		quality_class VARCHAR,
		start_date INTEGER,
		end_date INTEGER))
			]
		--When activity record does change, we want add a new record
		WHEN did_change IS NULL
		 THEN ARRAY[CAST(ROW(COALESCE(is_active_last_year, is_active_this_year),
		 COALESCE(quality_class_last_year, quality_class_this_year),
		start_date,
		end_date) AS ROW(is_active BOOLEAN,
		quality_class VARCHAR,
		start_date INTEGER,
		end_date INTEGER))]
		--We want to capture activity recrods that fell off, and new incoming activity
	END AS change_array
FROM
	combined
)
 SELECT
	actor,
	actor_id,
	arr.quality_class,
	arr.is_active,
	arr.start_date,
	arr.end_date,
	current_year
FROM
	changes
CROSS JOIN UNNEST(change_array) AS arr
