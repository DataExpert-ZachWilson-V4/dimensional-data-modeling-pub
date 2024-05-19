--query 1, schema for Cumulative Table
CREATE TABLE devpatel18.actors (
    actor VARCHAR,
    actor_id VARCHAR,
    films ARRAY(ROW(
    year INT,
        film VARCHAR,
        votes INT,
        rating DOUBLE,
        film_id VARCHAR
    )),
    quality_class VARCHAR,
    is_active BOOLEAN,
    current_year INT
)