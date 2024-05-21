CREATE OR REPLACE TABLE jb19881.actors_history_scd (
    actor varchar COMMENT 'actor''s name',
    actor_id varchar NOT NULL COMMENT 'Unique identifier for each actor, part of the primary key in actor_films dataset.',
    quality_class varchar,
    is_active boolean,
    start_date date COMMENT 'Marks the beginning of a particular state (quality_class/is_active) needed for SCD Type 2 tables.',
    end_date date COMMENT 'Marks the end of a particular state (quality_class/is_active) needed for SCD Type 2 tables.',
    current_year integer
)
COMMENT 'Data is sourced from the actors table created by query_1.sql and populated by query_2.sql. The actors_history_scd table is an SCD Type 2 table with one row per actor per current_year and is partitioned by current_year'
WITH
    (
        -- The Parquet file format is used to optimize for analytical query loads
        format = 'PARQUET',
        -- Partitioned by 'current_year' for efficient time-based data processing and analysis.
        partitioning = ARRAY['current_year']
    )