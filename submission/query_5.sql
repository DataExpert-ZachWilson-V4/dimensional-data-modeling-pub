INSERT INTO jessiii.actors_history_scd
-- Retrieves all entries from the history table for the previous year (2001).
WITH last_year_scd AS (
  SELECT * FROM jessiii.actors_history_scd
  WHERE current_year = 2001
),
-- Fetches all current year (2002) data from the actors table to be processed for changes.
current_year_scd AS (
  SELECT * FROM jessiii.actors
  WHERE current_year = 2002
),
-- Merges last year's historical data with this year's data, assessing changes in 'is_active' and 'quality_class'.
combined AS (
SELECT
  COALESCE(ly.actor, cy.actor) as actor,
  COALESCE(ly.actor_id, cy.actor_id) as actor_id,
  COALESCE(ly.start_date, cy.current_year ) as start_date,
  COALESCE(ly.end_date, cy.current_year) as end_date,
  -- General flag indicating any change in 'is_active' or 'quality_class'.
  CASE 
    WHEN ly.is_active <> cy.is_active OR ly.quality_class <> cy.quality_class THEN 1
    WHEN ly.is_active = cy.is_active AND ly.quality_class = cy.quality_class THEN 0
  END AS did_change,
  ly.is_active as is_active_last_year,
  cy.is_active as is_active_this_year,
  ly.quality_class as quality_last_year,
  cy.quality_class as quality_this_year,  
  2002 as current_year
  FROM last_year_scd ly
  FULL OUTER JOIN current_year_scd cy
  ON ly.actor_id = cy.actor_id
  AND ly.end_date + 1 = cy.current_year
),
-- Generates arrays of historical records, updated or unchanged, based on detected changes.
changes AS (
SELECT 
  actor,
  actor_id,  
  current_year,
  -- Constructs an array of historical rows based on whether there was a change or not.
  CASE WHEN did_change = 0
  THEN ARRAY[
  CAST(ROW(
	    is_active_last_year,
	    quality_last_year,
	    start_date,
	    end_date + 1)
	AS ROW(
		is_active boolean, 
		quality_class varchar, 
		start_date integer, 
		end_date integer))]
  WHEN did_change = 1
  THEN ARRAY[
	  CAST(ROW(
			is_active_last_year,
			quality_last_year,
		    start_date,
		    end_date)
		AS ROW(
		    is_active boolean,
		    quality_class varchar,
		    start_date integer,
		    end_date integer)),
        CAST(ROW(
	        is_active_this_year,
	        quality_this_year,
	        current_year,
	        current_year)
	    AS ROW(
           is_active boolean,
            quality_class varchar,
            start_date integer,
            end_date integer))
        ]
    WHEN did_change IS NULL
        THEN ARRAY[CAST(ROW(
            COALESCE(is_active_last_year, is_active_this_year),
            COALESCE(quality_last_year, quality_this_year),
            start_date,
            end_date
            ) AS ROW(is_active boolean, quality_class varchar, start_date integer, end_date integer))
           ]
    END as change_array
FROM combined)
-- Final SELECT to insert updated or unchanged historical records into the history table.
SELECT 
actor,
actor_id,
arr.quality_class, 
arr.is_active,
arr.start_date,
arr.end_date,
current_year
FROM changes
CROSS JOIN UNNEST(change_array) as arr
