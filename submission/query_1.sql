CREATE OR REPLACE TABLE mposada.actors (
    actor VARCHAR,
    actor_id VARCHAR,
    films ARRAY(ROW(  --STRUCT ARRAY FOR ALL FILMS OF CORRESPONDING ACTOR UP TO CURRENT YEAR
        film VARCHAR,
        votes INT,
        rating DOUBLE,
        film_id VARCHAR
    )),
    quality_class VARCHAR,
    is_active BOOLEAN,
    current_year INT
)
WITH
  (
    FORMAT = 'PARQUET',
    partitioning = ARRAY['current_year']  -- PARTITION BY CURRENT YEAR
  )
