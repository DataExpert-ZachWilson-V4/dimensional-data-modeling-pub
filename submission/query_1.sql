CREATE TABLE IF NOT EXISTS actors (
  actor VARCHAR,
  -- first and last name of the actor
  actor_id VARCHAR,
  -- unique actor id
  films ARRAY(
    ROW(
      film VARCHAR, -- name of the film
      film_id VARCHAR, -- unique film id
      film_year INTEGER, -- year the film was released
      votes INTEGER, -- number of votes the film received
      rating DOUBLE -- average rating of the film
    )
  ),
  -- array of films actor took part during the year, contains film name, unique film id, year the film was released, number of votes and average rating
  quality_class VARCHAR,
  -- word representation of average movie rating for this actor during his/her last active year
  is_active BOOLEAN,
  -- whether actor took part in any film during current_year
  current_year INTEGER -- year this row is representing
) WITH (
  FORMAT = 'PARQUET',
  partitioning = ARRAY ['current_year']
)