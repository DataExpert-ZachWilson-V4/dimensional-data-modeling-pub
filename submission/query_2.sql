INSERT INTO
	dswills94.actors
	--table with actors data
WITH last_year AS (
--Common Table Expression (CTE) to get data from the previous year
SELECT
		*
FROM
		dswills94.actors
	--table with actors data
WHERE
		current_year = 1913
	--current year as of last year
),
	this_year AS (
--CTE to get data this year
SELECT
	actor,
	--name of actor
	actor_id,
	--id of actor
		YEAR,
	--year film was released
	ARRAY_AGG(ROW(YEAR,
	--aggregate array as actor can be in multiple movies in year
	film,
	--name of film
	votes,
	--votes received for film
	rating,
	--rating received for film
	film_id)) AS films,
	--id of film
	AVG(rating) AS avg_rating
	--take average rating of films within year
FROM
		bootcamp.actor_films
	--table with films data
WHERE
		YEAR = 1914
	--current year as of this year
GROUP BY
		actor,
		actor_id,
		YEAR
	--Group By actor, actor_id, year if multiple films in this year
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
		--if average of film rating very high then star quality_class
		WHEN avg_rating > 7
		AND avg_rating <= 8 THEN 'good'
		--if average of film rating high then good quality_class
		WHEN avg_rating > 6
		AND avg_rating <= 7 THEN 'average'
		--if average of film rating is okay then average quality_class
		ELSE 'bad'
		--all else average of film rating is bad
	END AS quality_class,
	--qualify average rating by class
	ty.year IS NOT NULL is_active,
	--if actors have film released in year then active
	COALESCE(ty.year, ly.current_year + 1) AS current_year
	--find first non null value from this year and last year as current year
FROM
	last_year ly
FULL OUTER JOIN this_year ty
  ON
	ly.actor_id = ty.actor_id
	--to match up the actor films from this year and last year, as well as new active actors and retired old actors
