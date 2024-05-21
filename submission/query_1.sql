CREATE TYPE film AS (
    film VARCHAR(255),
    votes INT,
    rating DECIMAL(3, 2),
    film_id INT
);

CREATE TABLE actors (
    actor_id INT PRIMARY KEY,
    actor VARCHAR(255) NOT NULL, 
    film film[] NOT NULL, 
    quality_class VARCHAR(10) CHECK (quality_class IN ('star', 'good', 'average', 'bad')), 
    is_active BOOLEAN NOT NULL, 
    current_year INT NOT NULL
)
