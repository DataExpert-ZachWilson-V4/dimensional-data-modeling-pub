create table phabrahao.actors_history_scd (
  actor varchar,
  actor_id varchar,
  quality_class varchar,
  is_active boolean,
  start_date integer,
  end_date integer,
  current_year integer
) WITH (
  FORMAT = 'PARQUET',
  partitioning = ARRAY ['current_year']
)