insert into nonasj.actors
with actors_last_yr as (
  select * from nonasj.actors where current_year = 1939
),
actors_this_yr as (
  select
    actor,
    actor_id,
    ARRAY_AGG(
      ROW(
        film,
        film_id,
        votes,
        rating,        
        year
      )
    ) AS films,
    AVG(rating) AS avg_rating,
    year
  FROM
    bootcamp.actor_films
  WHERE
    rating is not null
    and year = 1940
  GROUP BY
    actor,
    actor_id,
    year
)
select
  coalesce(aly.actor, aty.actor) as actor,
  coalesce(aly.actor_id, aty.actor_id) as actor_id,
  case
    when aty.films is null then aly.films
    when aty.films is not null
    and aly.films is null then aty.films
    when aty.films is not null
    and aly.films is not null then aty.films || aly.films
  end as films,
  CASE
    WHEN avg_rating > 8 THEN 'star'
    WHEN avg_rating > 7 THEN 'good'
    WHEN avg_rating > 6 THEN 'average'
    ELSE 'bad'
  END AS quality_class,
  aty.year is not null as is_active,
  coalesce(aty.year, aly.current_year + 1) as current_year
from
  actors_last_yr aly FULL
  OUTER JOIN actors_this_yr aty ON aly.actor_id = aty.actor_id
