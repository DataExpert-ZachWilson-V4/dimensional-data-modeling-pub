CREATE OR REPLACE FUNCTION populate_actors_table() RETURNS VOID AS $$
DECLARE 
    target_year INT; 
BEGIN 
    FOR target_year IN (
        SELECT DISTINCT year FROM actor_films
    )

    LOOP 

        WITH actor_yyyy_data AS (
            SELECT actor_id, 
                actor, 
                array_agg(ROW(film, votes, rating, film_id))
                CASE WHEN AVG(rating) > 8 THEN 'star'
                     WHEN AVG(rating) > 7 and AVG(rating) <= 8 then 'good'
                     WHEN AVG(rating) > 6 and AVG(rating) <= 7 then 'average'
                     WHEN AVG(rating) <= 6 then 'bad' END AS quality_class, 
                MAX(year) = EXTRACT(YEAR FROM CURRENT_DATE) AS is_active
                year AS current_year,
            FROM actor_films
            GROUP BY actor_id, actor, current_year
        )
        INSERT INTO actors (actor_id, actor, films, quality_class, is_active, current_year)
        SELECT actor_id, 
                actor, 
                films, 
                quality_class, 
                is_active, 
                current_year 
        FROM actor_yyyy_data; 
    END LOOP;
END; 
$$

