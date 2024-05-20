CREATE TABLE sravan.actors_history_scd (
  actor_id INT PRIMARY KEY REFERENCES sravan.actors(actor_id),  -- Foreign key to actors table
  quality_class VARCHAR(10),
  is_active BOOLEAN,
  start_date DATE,
  end_date DATE DEFAULT NULL
);
