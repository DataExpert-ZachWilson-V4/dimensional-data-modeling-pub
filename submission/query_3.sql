-- DDL statement to creat an 'actors_history_scd'

CREATE OR REPLACE TABLE steve_hut.actors_history_scd (
    actor VARCHAR,
    actor_id VARCHAR,
    quality_class VARCHAR,
    is_active BOOLEAN,
    start_date INTEGER,
    end_date INTEGER
)
WITH (
    FORMAT = 'PARQUET',
    PARTITIONING = ARRAY['end_date']
)