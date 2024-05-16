INSERT INTO actors_history_scd
WITH last_year_scd AS (SELECT *
                       FROM actors_history_scd
                       WHERE current_year = 1913),
     current_year_scd AS (SELECT *
                          FROM actors
                          WHERE current_year = 1914),
     combined AS (SELECT COALESCE(ly.actor, cy.actor)             AS actor,
                         COALESCE(ly.quality_class, cy.quality_class)             AS quality_class,
                         COALESCE(ly.start_year, cy.current_year) AS start_year,
                         COALESCE(ly.end_year, cy.current_year)   AS end_year,
                         CASE
                             WHEN ly.is_active <> cy.is_active OR ly.quality_class <> cy.quality_class THEN 1
                             WHEN ly.is_active = cy.is_active  AND ly.quality_class = cy.quality_class  THEN 0
                         END                                            AS did_change,
                         ly.quality_class                               AS quality_class_last_year,
                         cy.quality_class                               AS quality_class_current_year,
                         ly.is_active                                   AS is_active_last_year,
                         cy.is_active                                   AS is_active_current_year,
                         1914                                           AS current_year
                  FROM last_year_scd ly
                           FULL OUTER JOIN current_year_scd cy ON ly.actor = cy.actor
                                AND ly.end_year + 1 = cy.current_year),
     changes AS (SELECT actor,
                        current_year,
                        CASE
                            WHEN did_change = 0 THEN ARRAY[
                                CAST(
                                        ROW(quality_class_last_year, is_active_last_year, start_year, end_year + 1)
                                            AS ROW (quality_class varchar, is_active boolean, start_year integer, end_year integer)
                                    )
                                ]
                            WHEN did_change = 1 THEN ARRAY[
                                CAST(
                                        ROW(quality_class_last_year, is_active_last_year, start_year, end_year)
                                            AS ROW (quality_class varchar, is_active boolean, start_year integer, end_year integer)
                                    ),
                                CAST(
                                        ROW(quality_class_current_year, is_active_current_year, current_year, current_year)
                                            AS ROW(quality_class varchar, is_active boolean, start_year integer, end_year integer)
                                    )
                                ]
                            WHEN did_change IS NULL THEN ARRAY[
                                CAST (
                                        ROW (COALESCE(quality_class_last_year, quality_class_current_year), COALESCE (is_active_last_year, is_active_current_year), start_year, end_year)
                                            AS ROW (quality_class varchar, is_active boolean, start_year integer, end_year integer)
                                    )
                                ]
                        END AS change_array
                FROM combined
                )
SELECT actor,
       arr.quality_class,
       arr.is_active,
       arr.start_year,
       arr.end_year,
       current_year
FROM changes
         CROSS JOIN UNNEST(change_array) AS arr