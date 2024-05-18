create
or replace table sarneski44638.actors (
    actor VARCHAR, -- actor name
    actor_id VARCHAR, -- unique id for actor
    -- aggregate films the actor was into an array (including films with release dates up to and the including current_year)
    films ARRAY(
        row(
            film VARCHAR, -- film name
            year INTEGER, -- year film was released
            votes INTEGER, -- # votes film receieved
            rating DOUBLE, -- film rating
            film_id VARCHAR -- unique id for film
        )
    ),
    quality_class VARCHAR, -- categorical variable based on average rating of movies for this actor in their most recent year
    is_active BOOLEAN, -- indicator whether actor is active making films in current year
    current_year INTEGER
)
with
    (
        FORMAT = 'PARQUET',
        partitioning = ARRAY['current_year']
    )
    --comment for grader