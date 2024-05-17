/* 
Cumulative Table Computation Query (query_2)
Prompt:
Write a query that populates the actors table one year at a time.
Note: the cumulative table DDL harathi.actors, created as part of query_1.sql should be populated in the following steps:
1. Create a CTE for last year's data
2. Create a CTE for this year's data
3. TO populate the cumulative table follow these three steps and backfill from min to max year values in bootcamp.actor_films table
  3a. Join these two CTEs using FULL OUTER JOIN to make a dataset containing this year and last years data (select min(year), max(year) from bootcamp.actor_films  --1914,	2021)
  3b. Coalesce the dimensions that are not changing
  3c. Combine all rows in the array films grain using concat, so that this col contains all the cumulative data of changing dimensions data year after year. 
      Later this column can be exploded using REDUCE or transofrom to draw various metrics.
4. Insert this dataset to the cumulative table harathi.actors
*/

insert into harathi.actors
with actors_last_yr as (
  select
    *
  from
    harathi.actors
  where
    current_year = 1939
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
