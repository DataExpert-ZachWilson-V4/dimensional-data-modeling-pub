INSERT INTO mamontesp.actors 
WITH last_year AS (
	SELECT
		  actor
		, actor_id
		, films 
		, quality_class
		, is_active
		, current_year
	FROM mamontesp.actors
	WHERE current_year = 1925
),
this_year AS (
	SELECT 
		  actor
		, actor_id
		, ARRAY_AGG(ROW(film, votes, rating, film_id)) AS films
		, CASE  
			WHEN AVG(rating) > 8
				THEN 'star'
			WHEN AVG(rating) > 7
				THEN 'good'
			WHEN AVG(rating) > 6
				THEN 'average'
			WHEN AVG(rating) <= 6
				THEN 'bad'
			END AS quality_class
		, True AS is_active
		, year as current_year
	FROM bootcamp.actor_films
	WHERE year = 1926
	GROUP BY actor, actor_id, year
)
SELECT 
	  COALESCE(ly.actor, ty.actor) AS actor
	, COALESCE(ly.actor_id, ty.actor_id) AS actor_id
	, COALESCE(ly.films, ARRAY[]) || COALESCE(ty.films, ARRAY[]) AS films 
	, COALESCE(ty.quality_class, ly.quality_class) AS quality_class
	, COALESCE(ty.is_active, False) AS is_active
	, 1926 AS current_year
FROM this_year AS ty
FULL OUTER JOIN last_year AS ly
ON ty.actor_id = ly.actor_id 

-- Test queries
	--, COALESCE(ly.films, ARRAY[]) || ty.films AS films 
-- SELECT * FROM mamontesp.actors limit 10