CREATE TABLE mamontesp.actors_history_scd (
	  actor VARCHAR
	, quality_class VARCHAR
	, is_active BOOLEAN
	, start_date INTEGER
	, end_date INTEGER 
)
WITH (
	  format = 'PARQUET'
	, partitioning = ARRAY['start_date']
)