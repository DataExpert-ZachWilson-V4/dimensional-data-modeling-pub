-- DDL query to create an actors table

CREATE OR REPALCE TABLE steve_hut.actors (
  actor VARCHAR,
  actor_ID VARCHAR,
  films ARRAY(ROW(
    film VARCHAR,
    votes INTEGER,
    rating DOUBLE,
    film_ID VARCHAR
  )),
  quality_class VARCHAR,
  is_active BOOLEAN,
  current_year INTEGER
)