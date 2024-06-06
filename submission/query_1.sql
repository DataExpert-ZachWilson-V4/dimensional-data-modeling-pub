/**
 * Creates a table named "actors" in the "bhakti" schema.
 * The table has the following columns:
 * - actor_id: integer
 * - actor: string
 * - films: array of rows, each containing the following columns:
 *   - film: string
 *   - votes: integer
 *   - rating: double
 *   - film_id: integer
 * - quality_class: string
 * - is_active: boolean
 * - current_year: integer
 * The table is stored in the "PARQUET" format and partitioned by the "current_year" column.
 */
CREATE TABLE bhakti.actors (
    actor VARCHAR,
    actor_id VARCHAR,
    films ARRAY<ROW(
        film VARCHAR,
        votes INT,
        rating DOUBLE,
        film_id varchar
    )>,
    quality_class VARCHAR,
    is_active BOOLEAN,
    current_year INT
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['current_year']
)

