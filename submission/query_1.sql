CREATE TABLE nattyd.actors (
    actor VARCHAR,
    actorid VARCHAR,
    films ARRAY (
      ROW (
        film VARCHAR,
        votes INTEGER,
        rating DECIMAL,
        filmid VARCHAR
      )
    ),
    quality_class VARCHAR,
    is_active BOOLEAN,
    current_year INTEGER
)
WITH (
    FORMAT = 'PARQUET',
    partitioning = ARRAY['current_year']
)