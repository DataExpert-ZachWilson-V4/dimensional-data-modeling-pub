-- Creating table actors_history_scd from the fields in actors_films dataset

CREATE OR REPLACE TABLE shruthishridhar.actors_history_scd (
    actor VARCHAR,  -- actor's name
    quality_class VARCHAR,  -- bucketing of average rating of films the most recent year
    is_active BOOLEAN,  -- indicates if the actor is currently active
    start_date INTEGER,  -- the starting year for this actor
    end_date INTEGER,  -- the ending year for this actor
    current_year INTEGER  -- the year this row represents for this actor
)
WITH (
    format = 'PARQUET', -- setting data format as PARQUET
    partitioning = ARRAY['current_year']  -- partitioning this data based on current_year
)