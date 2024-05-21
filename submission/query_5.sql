INSERT INTO dswills94.actors_history_scd
WITH last_year_scd AS (
--create CTE to use last year data
SELECT
	*
FROM
	dswills94.actors_history_scd
	--pull from scd table
WHERE
	current_year = 1917
	--current year as of last year
),
 curent_year_scd AS (
--create CTE to use current year data
SELECT
	*
FROM
	dswills94.actors
	--pull from actors table
WHERE
	current_year = 1918
	--current year as of this year
),
combined AS (
--We to match on the records that have potential to change
SELECT 
	COALESCE(ly.actor, cy.actor) AS actor,
	--pull first non null actor name from last year and current year
	COALESCE(ly.actor_id, cy.actor_id) AS actor_id,
	--pull first non null actor id from last year and current year
	COALESCE(ly.start_date, cy.current_year) AS start_date,
	--pull first non null start_date from last year and current year from current year
	COALESCE(ly.end_date, cy.current_year) AS end_date,
	--pull first non null end date from last year and current year from current year
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
	--active last year flag
	cy.is_active AS is_active_this_year,
	--active this year flag
	ly.quality_class AS quality_class_last_year,
	--quality class of films last year
	cy.quality_class AS quality_class_this_year,
	--quality class of films this year
	1918 AS current_year
	--set current year
FROM
	last_year_scd ly
FULL OUTER JOIN curent_year_scd cy
ON
	ly.actor_id = cy.actor_id
	AND ly.end_date + 1 = cy.current_year
	-- capturing intersecting actors data from this year and last year, brand new actors, and old retired actors by matching actor id and current year
),
changes AS (
SELECT
	actor,
	--name of actor
	actor_id,
	--id of actor
	current_year,
	--current year of analysis
	CASE
		WHEN did_change = 0
		THEN ARRAY[
		CAST(
		ROW(
		is_active_last_year,
		quality_class_last_year,
		start_date,
		end_date + 1)
		AS ROW(
		is_active BOOLEAN,
		quality_class VARCHAR,
		start_date INTEGER,
		end_date INTEGER)
		)
		]
		--When activity record doesn't change, but we want to extend old record with updated interval
		WHEN did_change = 1
		THEN ARRAY[
		CAST(
		ROW(
		is_active_last_year,
		quality_class_last_year,
		start_date,
		end_date)
		AS ROW(
		is_active BOOLEAN,
		quality_class VARCHAR,
		start_date INTEGER,
		end_date INTEGER)
		),
		CAST(
		ROW(
		is_active_this_year,
		quality_class_this_year,
		current_year,
		current_year)
		AS ROW(
		is_active BOOLEAN,
		quality_class VARCHAR,
		start_date INTEGER,
		end_date INTEGER)
		)
			]
		--When activity record does change, we want add a new record
		WHEN did_change IS NULL
		 THEN ARRAY[
		 CAST(
		 ROW(
		 COALESCE(is_active_last_year, is_active_this_year),
		 COALESCE(quality_class_last_year, quality_class_this_year),
		start_date,
		end_date)
		AS ROW(is_active BOOLEAN,
		quality_class VARCHAR,
		start_date INTEGER,
		end_date INTEGER)
		)
		]
		--We want to capture activity recrods that fell off, and new incoming activity
	END AS change_array
FROM
	combined
)
 SELECT
	actor,
	--name of actor
	actor_id,
	--id of actor
	arr.quality_class,
	--quality class array
	arr.is_active,
	--active flag array
	arr.start_date,
	--start SCD change date
	arr.end_date,
	--end SCD cahnge date
	current_year
	--current year of analysis
FROM
	changes
CROSS JOIN UNNEST(change_array) AS arr
	--explode array out to see rows
