Insert INTO mymah592.actors_history_scd
WITH
    last_year_scd AS(
    Select  
         *
    FROM
        mymah592.actors_history_scd
        WHERE
        current_year = 2001
    ),
    current_year_scd AS(
        SELECT
        *
        FROM 
        mymah592.actors_history_scd
        WHERE
        current_year = 2002
    ),
    Combined AS(
        SELECT
        COALESCE(ly.actor, cy.actor) AS actor,   
        COALESCE(ly.actor_ID, cy.actor_ID) AS actor_ID,
        COALESCE(ly.start_year, cy.start_year) AS start_year,
        COALESCE(ly.end_year, cy.end_year) AS end_year,
        CASE
          WHEN (ly.is_active <> cy.is_active) THEN 1
          WHEN (ly.is_active = cy.is_active) THEN 0
          END as did_change,
        CASE   
          WHEN (ly.quality_class <> cy.quality_class) THEN 1
          WHEN (ly.quality_class = cy.quality_class) THEN 0
          END as qc_did_change,

          ly.is_active as is_active_last_year,
          cy.is_active as is_active_this_year,
          ly.quality_class as quality_class_last_year,
          cy.quality_class as quality_class_this_year,
          2002 as current_year
    FROM
        last_year_scd ly
        FULL OUTER JOIN current_year_scd cy ON ly.actor_ID = cy.actor_ID
        and ly.end_year + 1 = cy.current_year
    ),
    Changes AS(
        SELECT
          actor,
          actor_ID,
          current_year,
          CASE
            When did_change = 0 THEN ARRAY[
                CAST(
                    ROW(
                        is_active_last_year,
                        quality_class_last_year,
                        start_year,
                        end_year +1) 
                    AS ROW(
                            is_active BOOLEAN,
                            quality_class VARCHAR,
                            start_year INTEGER,
                            end_year INTEGER
                        )
                )
            ]
            WHEN did_change = 1 THEN ARRAY[
                CAST(
                    ROW(is_active_last_year, quality_class_last_year, start_year, end_year) AS ROW(
                        is_active BOOLEAN,
                        quality_class VARCHAR,
                        start_year INTEGER,
                        end_year INTEGER
                    )
                ),
                CAST(
                    ROW(
                        is_active_this_year,
                        quality_class_this_year,
                        current_year,
                        current_year
                    ) AS ROW(
                        is_active BOOLEAN,
                        quality_class VARCHAR,
                        start_year INTEGER,
                        end_year INTEGER
                    )
                )
            ]
            WHEN did_change IS NULL Then ARRAY[
                CAST(
                    ROW(
                        COALESCE(is_active_last_year, is_active_this_year),COALESCE(quality_class_last_year, quality_class_this_year), 
                        start_year,
                        end_year
                    ) AS ROW(
                        is_active BOOLEAN,
                        quality_class VARCHAR,
                        start_year INTEGER,
                        end_year INTEGER
                    )
                )
            ]
            END as Change_Array
            from
            combined
    )
    SELECT
    actor,
    actor_id,
    ARR.quality_class,
    ARR.is_active,
    ARR.start_year,
    ARR.end_year,
    current_year
    FROM
    changes
    CROSS JOIN UNNEST (Change_Array) as ARR
