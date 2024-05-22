CREATE OR REPLACE TABLE actors_history_scd (
    -- Name of the actor
    actor VARCHAR, 
    -- categorical bucketing of the average rating of the movies for this actor in their most recent year.          
    quality_class VARCHAR,   
    -- Indicates whether an actor is currently active in the film industry (i.e., making films this year).
    is_active BOOLEAN,   
    -- Start date of the actor's history record     
    start_date INTEGER,    
   -- End date of the actor's history record     
    end_date INTEGER,
    -- The year this row represents for the actor            
    current_year INTEGER     
)
WITH
(
    FORMAT = 'PARQUET',      
    -- Partitioned by 'current_year' for efficient time and cost-based analysis
    partitioning = ARRAY['current_year']  
)
