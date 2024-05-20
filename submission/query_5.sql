INSERT INTO mamontesp.actors_history_scd
WITH last_year_report AS (
SELECT 
	  actor
	, quality_class
	, is_active
	, start_date
	, end_date
FROM mamontesp.actors_history_scd
WHERE end_date = 1919  
), 
this_year_report AS (
SELECT
	  actor
	, quality_class
	, is_active
	, current_year
FROM mamontesp.actors
WHERE current_year = 1920
),

combined_years AS (
SELECT 
	  COALESCE(ly.actor, ty.actor) as actor
	, ty.quality_class
	, ly.quality_class AS last_year_quality_class
	, ty.is_active
	, ly.is_active AS last_year_is_active
	, CASE 
		WHEN ly.quality_class <> ty.quality_class
			THEN True
		WHEN ly.quality_class = ty.quality_class
			THEN False
	END AS has_changed_quality_class
	, CASE
		WHEN ly.is_active <> ty.is_active
			THEN True
		WHEN ly.is_active = ty.is_active
			THEN False
	END AS has_changed_is_active
	, COALESCE(ly.start_date, ty.current_year) AS start_date
	, COALESCE( ly.end_date, ty.current_year) AS end_date
	, ty.current_year

FROM last_year_report AS ly
FULL OUTER JOIN this_year_report as ty
ON ly.actor = ty.actor
),

combined_feature_difference AS (
SELECT 
	  actor
	, quality_class
	, last_year_quality_class
	, is_active
	, last_year_is_active
	, CASE 
		WHEN has_changed_quality_class = False and has_changed_is_active = False
			THEN False
		WHEN has_changed_quality_class = True or has_changed_is_active = True
			THEN True
	END AS have_features_changed
	, start_date
	, end_date
	, current_year
FROM combined_years
), 

compiled_changes AS (
SELECT
	  actor
	, CASE
		WHEN not have_features_changed
			THEN ARRAY[
				CAST(ROW(last_year_quality_class, last_year_is_active, start_date, current_year) 
				AS ROW(quality_class VARCHAR, is_active BOOLEAN, start_date INTEGER, end_date INTEGER))
			] 
		WHEN have_features_changed
			THEN ARRAY[
			CAST(ROW(last_year_quality_class, last_year_is_active, start_date, end_date) AS
			ROW(quality_class VARCHAR, is_active BOOLEAN, start_date INTEGER, end_date INTEGER)),
			CAST(ROW(quality_class, is_active, current_year, current_year) AS
			ROW(quality_class VARCHAR, is_active BOOLEAN, start_date INTEGER, end_date INTEGER))
			]
		WHEN have_features_changed is NULL
			THEN ARRAY[
			CAST(ROW(COALESCE(last_year_quality_class, quality_class), COALESCE(last_year_is_active, is_active), start_date, end_date) AS
			ROW(quality_class VARCHAR, is_active BOOLEAN, start_date INTEGER, end_date INTEGER))
			]
		END AS changes_array
FROM combined_feature_difference
)

SELECT
	  actor
	, arr.quality_class
	, arr.is_active
	, arr.start_date
	, arr.end_date
FROM compiled_changes
CROSS JOIN UNNEST(changes_array) AS arr