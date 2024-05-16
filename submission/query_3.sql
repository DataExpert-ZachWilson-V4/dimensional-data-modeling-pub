CREATE TABLE IF NOT EXISTS actors_history_scd (
    actor VARCHAR,
    -- first and last name of the actor
    quality_class VARCHAR,
    -- word representation of average movie rating for this actor during his/her last active year in given period
    is_active BOOLEAN,
    -- whether actor took part in any film during given period
    start_date INTEGER,
    -- start year this dimension applies to
    end_date INTEGER,
    -- end year this dimension applies to
    current_year INTEGER
    -- current year for partitioning
) WITH (
    FORMAT = 'PARQUET',
    partitioning = ARRAY ['current_year']
)