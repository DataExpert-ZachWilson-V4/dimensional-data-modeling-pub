

INSERT INTO steve_hut.actors
WITH
last_year
AS
(
  SELECT
    *
FROM
    steve_hut.actors
WHERE current_year = 1916
)
,
this_year AS
(
  SELECT
    *
FROM
    steve_hut.actor_films
WHERE year = 1917
)
SELECT
    COALESCE(ly.actor, ty.actor) AS actor,
    COALESCE(ly.actor_id, ty.actor_id) AS actor_id,
    CASE
    WHEN ty.film IS NULL THEN ly.films
    WHEN ty.film IS NOT NULL AND ly.films IS NULL THEN ARRAY[ROW(
    ty.film, ty.votes, ty.rating, ty.film_id)] 
    WHEN ty.film IS NOT NULL AND ly.films IS NOT NULL THEN ARRAY[ROW(
    ty.film, ty.votes, ty.rating, ty.film_id)] || ly.films
  END AS films,
    NULL AS quality_class,
    ty.year
IS NOT NULL AS is_active,
  COALESCE
(ty.year, ly.current_year + 1) AS current_year
FROM last_year ly
FULL OUTER JOIN this_year ty ON ty.actor_id = ly.actor_id