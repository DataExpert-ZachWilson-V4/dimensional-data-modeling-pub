create or replace table actors(
actor varchar,
actor_id varchar,
films array(    --struct array for storing film variables for the actor
row(
film varchar,
votes integer,
rating double,
film_id varchar
)),
quality_class varchar,   --categorical variable for binning average ranges, hence varchar
is_active boolean,
current_year integer
)
with
  (
    format = 'PARQUET',
    partitioning = ARRAY['current_year']
  )
