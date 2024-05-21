CREATE TABLE actors_history_scd (
    actor_history_id INT PRIMARY KEY, 
    actor_id INT NOT NULL, 
    quality_class VARCHAR(10) CHECK (quality_class IN ('star', 'good', 'average', 'bad')),
    is_active BOOLEAN NOT NULL, 
    start_date DATE NOT NULL, 
    end_date DATE, 
    CONSTRAINT fk_actor FOREIGN KEY (actor_id) REFERENCES actors(actor_id)
)
