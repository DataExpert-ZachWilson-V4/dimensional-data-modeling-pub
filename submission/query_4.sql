INSERT INTO mamontesp.actors_history_scd
WITH last_year_report AS (
SELECT 
	  actor
	, quality_class
	, LAG(quality_class, 1) OVER (PARTITION BY actor ORDER BY current_year) AS quality_class_last_year
	, is_active
	, LAG(is_active, 1) OVER (PARTITION BY actor ORDER BY current_year) as was_active_last_year
	, current_year
FROM mamontesp.actors 
WHERE current_year < 1920 ),

difference_identificator AS (
SELECT 
	   actor
	 , quality_class
	 , CASE 
	 	WHEN quality_class <> quality_class_last_year 
	 	THEN true
	 	ELSE false 
	 END AS has_changed_quality_class
	 , is_active
	 , CASE 
	 	WHEN is_active <> was_active_last_year
	 	THEN true
	 	ELSE false
	 END AS has_changed_active
	 , current_year
FROM last_year_report
),
combined_difference AS (
SELECT
	  actor
	, quality_class
	, is_active
	, CASE 
		WHEN has_changed_quality_class = False AND has_changed_active = False
		THEN 0
		ELSE 1
	END AS have_features_change
	, current_year
FROM difference_identificator
),
streak_identifier AS (
SELECT
	  actor
	, quality_class
	, is_active
	, SUM(have_features_change) OVER (PARTITION BY actor ORDER BY current_year) AS streak
	, current_year
FROM combined_difference
)

SELECT 
	  DISTINCT actor
	, quality_class
	, BOOL_AND(is_active) AS is_active
	, MIN(current_year) AS start_date
	, MAX(current_year) AS end_date
FROM streak_identifier
GROUP BY actor, quality_class, streak


