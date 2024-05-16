--DDL statement to create an actors_history_scd table  for each actor in the actors table that tracks below fields

create table vaishnaviaienampudi83291.actors_history_scd
(
    actor_id varchar,
    quality_class varchar,
    is_active boolean,
    start_date integer, ----we can use either date or integer. In the actors_films it is integer. So going forward with the same.
    end_date integer,  --using integer after checking the data
    current_year integer --we can assume it to be current_date. Everything is in year format in dataset, so used current year
)
with (
  format = 'PARQUET'
)