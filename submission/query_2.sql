INSERT into emmaisemma.actors
WITH ly as(
    SELECT 
        actor,
        actor_id,
        films,
        quality_class,
        is_active,
        current_year
    FROM emmaisemma.actors
    WHERE current_year = 2000
),

ty as(
    SELECT
        actor,
        actor_id,
        year,
        ARRAY_AGG(ROW(film, votes, rating,film_id, year)) AS films, 
        case
            when AVG(rating) > 8 then 'star'
            when AVG(rating) > 7 then 'good'
            when AVG(rating) > 6 then 'average'
            else 'bad'
        END as quality_class 
        
    FROM bootcamp.actor_films
    where year = 2001
    GROUP BY actor, actor_id, year
)

SELECT 
    COALESCE(ty.actor, ly.actor) as actor,
    COALESCE(ty.actor_id, ly.actor_id) as actor,
    CASE WHEN ty.films is NUll THEN ly.films
        WHEN ty.films is NOT NUll and ly.films is Null THEN ty.films
        WHEN ty.films is NOT NULL and ly.films is Not Null THEN ty.films||ly.films end as films,
    COALESCE(ty.quality_class, ly.quality_class) as quality_class,
    CASE WHEN ty.films IS NULL THEN False ELSE TRUE END AS is_active,
    COALESCE(ty.year, ly.current_year+1) as current_year

FROM ty
FULL OUTER JOIN ly
ON ty.actor_id = ly.actor_id 
