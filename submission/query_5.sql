--Actors History SCD Table Incremental Backfill Query (query_5)

with ly_data as (select *
                 from hariomnayani88482.actors_history_scd
                 where current_year = 1919),
     cy_data as (select *
                 from hariomnayani88482.actors
                 where current_year = 1920),
     combined as (select coalesce(ly.actor, cy.actor)       as actor,
                         coalesce(ly.start_date, cy.current_year) as start_date,
                         coalesce(ly.end_date, cy.current_year)   as end_date,
                         ly.is_active                             as ly_is_active,
                         cy.is_active                             as cy_is_active,
                         ly.quality_class                         as ly_quality_class,
                         cy.quality_class                         as cy_quality_class,
                         case
                             when (ly.is_active <> cy.is_active) or (ly.quality_class <> cy.quality_class) then 1
                             when (ly.is_active = cy.is_active) or (ly.quality_class = cy.quality_class)
                                 then 0 end                       as did_change,
                         1920                                     as current_year
                  from ly_data ly
                           full outer join cy_data cy
                                           on ly.actor = cy.actor and ly.end_date + 1 = cy.current_year),
     cte as (select actor,
                    case
                        when did_change = 0 then
                            array [
                                cast(row (
                                    ly_is_active,
                                    ly_quality_class,
                                    start_date,
                                    end_date + 1
                                    ) as row(is_active boolean,quality_class varchar, start_date integer , end_date integer))]
                        when did_change = 1 then array [
                            cast(row (ly_is_active,
                                ly_quality_class,
                                start_date,
                                end_date) as row(is_active boolean,quality_class varchar, start_date integer , end_date integer)),
                            cast(row (
                                cy_is_active,
                                cy_quality_class,
                                current_year,
                                current_year
                                ) as row(is_active boolean,quality_class varchar, start_date integer , end_date integer))]
                        when did_change is null then array [
                            cast(row (
                                coalesce(ly_is_active, cy_is_active),
                                coalesce(ly_quality_class, cy_quality_class),
                                start_date,
                                end_date
                                ) as row(is_active boolean,quality_class varchar, start_date integer , end_date integer))]
                        end as change_array
             from combined)

select actor,
       change.*
from cte
         cross join unnest(change_array) as change
