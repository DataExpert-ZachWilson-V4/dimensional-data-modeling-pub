-- Second SQL command
INSERT INTO
  human.actors
WITH last_year AS (
  SELECT 
    * 
  FROM 
    human.actors
  WHERE
    current_year = 1913
    ),
    this_year AS (
    SELECT 
      actor,actor_id,
      --Aggregating films to rows
      ARRAY_AGG(ROW(film, year, votes, rating, film_id)) AS films,
    AVG(rating) AS avg_rating,
    year
    FROM 
    bootcamp.actor_films
    WHERE
      YEAR = 1914
      GROUP BY actor, actor_id,year
    )
  SELECT 
    COALESCE(ly.actor,ty.actor) AS   
    actor,
    COALESCE(ly.actor_id, ty.actor_id) AS actor_id,
    CASE
    WHEN ty.films IS NULL THEN ly.films
    WHEN ty.films is NOT NULL and ly.films is NULL THEN
    ty.films
    WHEN ty.filmS is NOT NULL and ly.films is NOT NULL THEN
    ty.films || ly.films
    END as films,
    --determining the quality category here

    CASE 
      WHEN avg_rating IS NULL THEN NULL
        WHEN avg_rating > 8 THEN 'star'
        WHEN avg_rating > 7 AND avg_rating <= 8 THEN 'good'
        WHEN avg_rating > 6 AND avg_rating <= 7 THEN 'average'
        ELSE 'bad'
    END AS quality_class,
    ty.YEAR is not NULL AS is_active,
    COALESCE(ty.YEAR, ly.current_year+1) AS current_year
  FROM
    last_year ly 
    FULL OUTER JOIN this_year ty ON   
    ly.actor_id = ty.actor_id