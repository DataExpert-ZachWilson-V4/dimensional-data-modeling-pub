CREATE OR REPLACE TABLE actors (
    actor VARCHAR NOT NULL, -- Actor name
    actor_id VARCHAR NOT NULL, -- Actor's ID
    films ARRAY(ROW( -- An array of struct with the following fields
        film VARCHAR, -- The name of the film.
        votes INTEGER, -- The number of votes the film received.
        rating DOUBLE, -- The rating of the film.
        film_id VARCHAR -- A unique identifier for each film.
    )),
    quality_class VARCHAR, -- A categorical bucketing of the average rating of the movies for this actor in their most recent year
    /*
        star Average rating > 8.
        good Average rating > 7 and ≤ 8.
        average Average rating > 6 and ≤ 7.
        bad Average rating ≤ 6.
    */
    is_active BOOLEAN, -- A BOOLEAN field that indicates whether an actor is currently active in the film industry (i.e., making films this year).
    current_year INTEGER -- The year this row represents for the actor
) WITH (
    format = 'PARQUET',
    partitioning = ARRAY['current_year']
)