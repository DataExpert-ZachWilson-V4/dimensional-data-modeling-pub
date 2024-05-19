--Cumulative Table Computation Query (query_2)

insert into sanniepatron.actors

-- Common Table Expressions (CTEs) for last year and this year data

-- CTE for data from last year  (data 1913-1929)
WITH last_year AS (  
    select * from sanniepatron.actors
    where current_year = 1916     
),

-- CTE for data from this year (data 1913-1930)
this_year AS (  
    SELECT 
    actor,
    actor_id,
    ARRAY_AGG(ROW(year, film, votes, rating, film_id)) as films, -- Aggregating film data into an array
    AVG(rating) as rating,
    year
     FROM bootcamp.actor_films
    WHERE year = 1917
    group by  actor,
    actor_id,
     year 
)

SELECT 
COALESCE(ly.actor,ty.actor)                                                         as actor,
COALESCE(ly.actor_id,ty.actor_id)                                                   as actor_id,

CASE
    when ty.year is null then ly.films                                              -- If this year data doesn't exist, use last year's films
    when ty.year is not null and ly.films is null then ty.films                     -- If last year data doesn't exist, use this year's films
    when ty.year is not null and ly.films is not null then ty.films ||  ly.films
end                                                                                 as films, 

CASE 
    WHEN rating > 8 THEN 'star'
    WHEN rating >7 and rating <= 8 THEN 'good'
    WHEN rating > 6 AND rating <= 7 THEN 'average'
ELSE 'bad'
END                                                                                 AS quality_class,
       
ty.year is not null                                                                 AS is_active,
COALESCE(ty.year, ly.current_year + 1)                                              as current_year
FROM last_year ly
full outer join this_year ty
ON ly.actor = ty.actor 
