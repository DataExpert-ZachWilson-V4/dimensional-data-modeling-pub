CREATE OR REPLACE TABLE tharwaninitin.actors_history_scd (
    actor VARCHAR, -- Name of the actor
    actor_id VARCHAR, -- Unique identifier for each actor
    quality_class VARCHAR, -- Classification of the actor's performance quality based on average rating in the most recent year: 'star' (>8), 'good' (>7 and ≤8), 'average' (>6 and ≤7), 'bad' (≤6)
    is_active BOOLEAN, -- Indicates whether the actor is currently active
    start_date INTEGER, -- Marks the beginning of a particular state (quality_class/is_active). Integral in Type 2 SCD to track changes over time.
    end_date INTEGER, -- Signifies the end of a particular state. Essential for Type 2 SCD to understand the duration of each state.
    current_year INTEGER -- The year this record pertains to. Useful for partitioning and analyzing data by year.
)
WITH (
    format = 'PARQUET', -- Data format for storage
    partitioning = ARRAY['current_year'] -- Partitioning key for optimization
)