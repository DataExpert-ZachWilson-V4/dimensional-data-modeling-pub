INSERT INTO mymah592.actors
    
WITH 
    temp as (
    Select
        Actor,
        Actor_id,
        ARRAY_AGG(ROW(film, votes, rating, film_id, YEAR)) AS films,
        AVG(rating) AS avg_rating,
        YEAR
    FROM
      bootcamp.actor_films
    WHERE
        Year = 2000
    Group by
        Actor,
        actor_ID,
        Year
    ),
          
    This_year as(
        SELECT actor,
        actor_id,
        year,
        films,
        Case
        when avg_Rating > 8 THEN 'star'
        when avg_Rating > 7 then 'Good'
        when avg_Rating > 6 then 'average'
        else 'bad' end as quality_class
        FROM 
            temp

    ),
    
    last_year AS (
        SELECT 
            *
        FROM
            mymah592.actors
        WHERE
            current_year = 1999
        )
-- COALESCE all the values that are not changing to handle NULLS
    SELECT
    COALESCE(ly.actor, ty.actor) as actor,
    COALESCE(ly.actor_ID, ty.actor_ID) as actor_ID,
    CASE 
        WHEN ty.year IS NULL THEN ly.films 
        WHEN ty.year IS NOT NULL and ly.films IS NULL THEN ty.films
       
        WHEN ty.year IS NOT NULL and ly.films IS NOT NULL THEN ly.films || ty.films

        END as films,
    COALESCE(ty.quality_class, ly.quality_class) as quality_class,
    (ty.year IS NOT NULL) as is_active,
    ty.year as current_year
    FROM last_year ly
    FULL OUTER JOIN This_year ty
    ON ly.actor_ID = ty.actor_ID

