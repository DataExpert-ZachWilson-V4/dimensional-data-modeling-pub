INSERT INTO
	dswills94.actors
WITH last_year AS (
	--Common Table Expression (CTE) to get data from the previous year
	SELECT
		*
	FROM
		dswills94.actors
	WHERE
		current_year = 1913
),
	this_year AS (
	SELECT
		actor,
		actor_id,
		year,
		ARRAY_AGG(ROW(YEAR,
		film,
		votes,
		rating,
		film_id)) AS films,
		--aggregate actor films for the year
		AVG(rating) AS avg_rating
	FROM
		bootcamp.actor_films
	WHERE
		year = 1914
	GROUP BY
		actor,
		actor_id,
		year
		--Group By actor_id if multiple films in this year
)
SELECT
	COALESCE(ly.actor, ty.actor) AS actor,
	--Handle null vaules
  COALESCE(ly.actor_id, ty.actor_id) AS actor_id,
	--Handle null vaules
  CASE
		WHEN ty.films IS NULL THEN ly.films
		--if actor hasn't starred in film this year, then pull forward actor's prior year film data
		WHEN ty.films IS NOT NULL
		AND ly.films IS NULL THEN ty.films
		--if actor starred in film this year and hasn't prior year, then aggreate this year's actor film data in an array
		WHEN ty.films IS NOT NULL
		AND ly.films IS NOT NULL THEN ty.films || ly.films
		--if actor starred in film this year and prior year, then aggreate this year' actor film data and concat last year's actor file data at end
	END AS films,
	CASE
		WHEN avg_rating > 8 THEN 'star'
		WHEN avg_rating > 7
		AND avg_rating <= 8 THEN 'good'
		WHEN avg_rating > 6
		AND avg_rating <= 7 THEN 'average'
		ELSE 'bad'
	END AS quality_class,
	ty.year IS NOT NULL is_active,
	COALESCE(ty.year, ly.current_year + 1) AS current_year
FROM
	last_year ly
FULL OUTER JOIN this_year ty
  ON
	ly.actor_id = ty.actor_id
