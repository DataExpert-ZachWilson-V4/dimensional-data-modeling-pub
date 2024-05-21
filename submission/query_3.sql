CREATE OR REPLACE TABLE dswills94.actors_history_scd (
--generate SCD table of actor history data
actor VARCHAR,
--name of actor
actor_id VARCHAR,
--id of actor primary key
quality_class VARCHAR,
--qualifier of film quality
is_active BOOLEAN,
--active flag
start_date INTEGER,
--this is the SCD Type 2 for idenmpotency
end_date INTEGER,
--this is the SCD Type 2 for idenmpotency
current_year INTEGER
--current year
)
WITH (
	format = 'PARQUET',
--usualy format to handle large data
partitioning = ARRAY['current_year']
--temporal compoenent by current year
)
