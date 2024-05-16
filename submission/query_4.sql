-- Write a "backfill" query that can populate the entire actors_history_scd table in a single query.
INSERT INTO actors_history_scd
WITH lagged AS ( -- CTE to hold previous years data
    SELECT
        actor,
        quality_class,
        LAG(quality_class) OVER (
            PARTITION BY actor
            ORDER BY
                current_year
        ) AS last_year_quality_class,
        CASE
            WHEN is_active THEN 1
            ELSE 0
        END as is_active,
        CASE
            WHEN LAG(is_active) OVER (
                PARTITION BY actor
                ORDER BY
                    current_year
            ) THEN 1
            ELSE 0
        END AS last_year_is_active,
        current_year
    FROM
        actors
    WHERE
        current_year <= 2012
),
streaked AS ( -- CTE to hold change for current year
    SELECT
        *,
        -- Create identifier only if is_active OR quality_class has changed
        SUM(
            IF(
                is_active <> last_year_is_active
                OR quality_class <> last_year_quality_class,
                1,
                0
            )
        ) OVER(
            PARTITION BY actor
            ORDER BY
                current_year
        ) AS identifier
    FROM
        lagged
)
-- Record the changes by identifier
SELECT
    actor,
    MAX(quality_class) as quality_class,
    MAX(is_active) = 1 AS is_active,
    MIN(current_year) AS start_date,
    MAX(current_year) AS end_date,
    2012 AS current_year
FROM
    streaked
GROUP BY
    actor,
    identifier