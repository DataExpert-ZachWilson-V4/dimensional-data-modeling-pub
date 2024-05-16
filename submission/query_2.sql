insert into lsleena.actors
WITH last_year AS
(
       -- get last year data from lsleena.actors
       SELECT *
       FROM   lsleena.actors
       WHERE  current_year= 2020 ) ,
this_year AS
(
         -- get this year data from bootcamp.actor_films
         SELECT   actor,
                  actor_id,
                  Array_agg(Row(year,film,votes,rating,film_id))AS films,
                  Avg(rating)                              AS avg_rating,
                  Max(year)                                AS year
         FROM     bootcamp.actor_films
         WHERE    year=2021
         GROUP BY actor,
                  actor_id )

SELECT          COALESCE(ly.actor,ty.actor)      AS actor,
                COALESCE(ly.actor_id,ty.actor_id)
                AS actor_id,
                CASE
                    WHEN ly.films IS NULL THEN ty.films
                    WHEN ty.films IS NULL THEN ly.films
                    ELSE ly.films || ty.films
                END AS films,
                -- populate quality_class based on average rating of current year's films
                CASE
                    WHEN ty.avg_rating > 8 THEN 'star'
                    WHEN ty.avg_rating > 7 THEN 'good'
                    WHEN ty.avg_rating > 6 THEN 'average'
                    ELSE 'bad'
                END AS quality_class,
                ty.year IS not NULL               AS is_active,
                COALESCE(ty.year,ly.current_year+1)AS current_year
FROM            last_year ly
FULL OUTER JOIN this_year ty
ON              ly.actor_id=ty.actor_id