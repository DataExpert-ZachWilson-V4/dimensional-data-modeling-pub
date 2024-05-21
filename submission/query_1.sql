CREATE OR REPLAACE TABLE dswills94.actors (  --we store actor data
  actor VARCHAR, --name of actor
  actor_id VARCHAR, -- id of actor primary key
  films ARRAY( --array of dimensions
    ROW(
      year INTEGER, --year film was released
      film VARCHAR, --name of film
      votes INTEGER, --votes film recieved
      rating DOUBLE, --rating film received
      film_id VARCHAR --id of film
    )
  ),
  quality_class VARCHAR, --qualifier of film ratings classified as star average bad
  is_active BOOLEAN, --is the actor active
  current_year INTEGER --current year
)
WITH
  (
    FORMAT = 'PARQUET', --usual format for large data
    partitioning = ARRAY['current_year'] --temporal aspect see year by year changes
  )
