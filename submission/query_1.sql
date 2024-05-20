CREATE TABLE sravan.actors (
  actor VARCHAR(255) NOT NULL,
  actor_id INT NOT NULL ,
  films ARRAY(ROW(
    film VARCHAR,
    votes INTEGER,
    rating DOUBLE,
    film_id VARCHAR
  )),
  quality_class VARCHAR(10) GENERATED ALWAYS AS (
    CASE AVG(f.rating) OVER (PARTITION BY actor_id, current_year)
      WHEN '> 8' THEN 'star'
      WHEN '> 7' THEN 'good'
      WHEN '> 6' THEN 'average'
      ELSE 'bad'
    END
  ) STORED,
  is_active BOOLEAN,
  current_year INT NOT NULL
);
