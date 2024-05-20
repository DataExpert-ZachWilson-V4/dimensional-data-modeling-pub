INSERT INTO whiskersreneewe.actors_history_scd
WITH last_year_scd AS (
SELECT * from whiskersreneewe.actors_history_scd
WHERE current_year = 1921
),
current_year_scd AS (
    select 
        *
    from whiskersreneewe.actors
    WHERE current_year = 1922
),

combined as (
SELECT 
COALESCE(ly.actor, cy.actor) as actor,
COALESCE(ly.actor_id, cy.actor_id) as actor_id,
cy.quality_class,
COALESCE(ly.start_date, cy.current_year) AS start_date,
COALESCE(ly.end_date, cy.current_year) AS end_date,
CASE
  WHEN ly.is_active <> cy.is_active THEN 1
  WHEN ly.is_active = cy.is_active THEN 0
END AS did_change,
ly.is_active AS is_active_last_year,
cy.is_active AS is_active_current_year,
2022 AS current_year
FROM last_year_scd ly 
FULL OUTER JOIN current_year_scd cy ON 
ly.actor_id = cy.actor_id AND 
ly.end_date + 1 = cy.current_year),

change AS (
SELECT 
 actor,
 actor_id,
 quality_class,
  CASE 
  WHEN did_change = 0 THEN ARRAY[CAST(
  ROW(is_active_last_year, start_date, end_date+1) AS ROW(is_active BOOLEAN, start_date INTEGER, end_date INTEGER))]
  WHEN did_change = 1 THEN ARRAY[
  CAST(ROW(is_active_last_year, start_date, end_date) AS ROW(is_active BOOLEAN, start_date INTEGER, end_date INTEGER)), CAST(ROW(is_active_current_year, current_year, current_year) AS ROW(is_active BOOLEAN, start_date INTEGER, end_date INTEGER))]
  WHEN did_change IS NULL THEN ARRAY[
  CAST(ROW(COALESCE(is_active_last_year, is_active_current_year), start_date, end_date) AS ROW(is_active BOOLEAN, start_date INTEGER, end_date INTEGER))] END AS change_arr
 FROM combined)
 
 SELECT actor, actor_id, quality_class,
 arr.*, 2022 AS current_year
 FROM change CROSS JOIN
 UNNEST(change_arr) as arr