CREATE TABLE dswills94.actors_history_scd (
	actor VARCHAR,
	actor_id VARCHAR,
	quality_class VARCHAR,
	is_active BOOLEAN,
	start_date INTEGER, --this is the SCD Type 2 for idenmpotency
	end_date INTEGER, --this is the SCD Type 2 for idenmpotency
	current_year INTEGER
)
WITH (
	format = 'PARQUET',
	partitioning = ARRAY['current_year']
)
