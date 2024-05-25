INSERT INTO actors_history_scd

WITH
  last_year_scd AS (
	SELECT
  	*
	FROM
  	actors_history_scd
	WHERE
  	current_year = 1918
  ),
  current_year_scd AS (
	SELECT
  	*
	FROM
  	actors
	WHERE
  	current_year = 1919
  ),
  combined AS (
	SELECT
	coalesce(ly.actor, cy.actor) actor,
  	coalesce(ly.actor_id, cy.actor_Id) actor_id,
  	coalesce(ly.start_date, cy.current_year) start_date,
  	coalesce(ly.end_date, cy.current_year) end_date,
  	CASE
    	WHEN ly.is_active <> cy.is_active
    	OR ly.quality_class <> cy.quality_class THEN 1
    	WHEN ly.is_active = cy.is_active
    	AND ly.quality_class = cy.quality_class THEN 0
  	END did_change,
  	ly.is_active AS is_active_last_year,
  	cy.is_active AS is_active_this_year,
  	ly.quality_class AS quality_class_last_year,
  	cy.quality_class AS quality_class_this_year,
  	1919 AS current_year
	FROM
  	last_year_scd ly
  	FULL OUTER JOIN current_year_scd cy ON ly.actor_id = cy.actor_id
  	AND ly.end_date + 1 = cy.current_year
  ),
  changes AS (
	SELECT
	actor,
  	actor_id,
  	current_year,
  	CASE
    	WHEN did_change = 0 THEN ARRAY[
      	CAST(
        	ROW(
          	is_active_last_year,
          	quality_class_last_year,
          	start_date,
          	end_date + 1
        	) AS Row(
          	is_active Boolean,
          	quality_class varchar,
          	start_date integer,
          	end_date integer
        	)
      	)
    	]
    	WHEN did_change = 1 THEN ARRAY[
      	CAST(
        	ROW(
          	is_active_last_year,
          	quality_class_last_year,
          	start_date,
          	end_date
        	) AS Row(
          	is_active Boolean,
          	quality_class varchar,
          	start_date integer,
          	end_date integer
        	)
      	),
      	CAST(
        	ROW(
          	is_active_this_year,
          	quality_class_this_year,
          	current_year,
          	current_year
        	) AS Row(
          	is_active Boolean,
          	quality_class varchar,
          	start_date integer,
          	end_date integer
        	)
      	)
    	]
    	WHEN did_change IS NULL THEN ARRAY[
      	CAST(
        	ROW(
          	COALESCE(is_active_last_year, is_active_this_year),
          	COALESCE(quality_class_last_year, quality_class_this_year),
          	start_date,
          	end_date
        	) AS Row(
          	is_active Boolean,
          	quality_class varchar,
          	start_date integer,
          	end_date integer
        	)
      	)
    	]
  	END changes_array
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
  CROSS JOIN UNNEST (changes_array) AS arr
  