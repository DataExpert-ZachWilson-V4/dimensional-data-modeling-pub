CREATE OR REPLACE TABLE dswills94.actors_history_scd (
--generate SCD table of actor history data
actor VARCHAR,
--name of actor
actor_id VARCHAR,
--unique id of actor
quality_class VARCHAR,
--categorical rating based on average rating in the most recent year
is_active BOOLEAN,
--indicates if the actor is currently active, based on making films this year.
start_date INTEGER,
--marks the beginning of a particular state (quality_class/is_active). Used in Type 2 SCD to track changes over time.
end_date INTEGER,
--signifies the end of a particular state. Used in Type 2 SCD to understand the duration of each state
current_year INTEGER
--the year this record pertains to. Useful for partitioning and analyzing data by year
)
WITH (
	format = 'PARQUET',
--data stored in PARQUET format for optimized analytics
partitioning = ARRAY['current_year']
--Partitioned by 'current_year' for efficient time-based analysis.
)
