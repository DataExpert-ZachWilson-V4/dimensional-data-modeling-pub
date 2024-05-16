CREATE OR REPLACE TABLE jessiii.actors (
	actor VARCHAR,
	actor_id VARCHAR,
	films ARRAY(ROW(
		film VARCHAR,
		votes INTEGER,
		rating DOUBLE,
		film_id VARCHAR,
		release_year INTEGER
	)),
	quality_class VARCHAR,
	is_active BOOLEAN,
	current_year INTEGER
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['current_year']
)

