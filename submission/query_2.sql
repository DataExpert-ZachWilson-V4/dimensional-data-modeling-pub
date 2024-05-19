insert into sanchit.actors
with ly_data as (select *
                 from sanchit.actors
                 where current_year = 1913),
     cy_data as (select actor,
                        actor_id,
                        case
                            when avg(rating) > 8 then 'star'
                            when avg(rating) > 7 then 'good'
                            when avg(rating) > 6 then 'average'
                            else 'bad'
                            end as qualicy_class,
    year, array_agg(row (year, film_id, film, votes, rating)) as films
from bootcamp.actor_films
where year = 1914
group by actor,
    actor_id,
    year)

select coalesce(ly.actor_id, cy.actor_id)           as actor_id,
       coalesce(ly.actor, cy.actor)                 as actor,
       coalesce(cy.qualicy_class, ly.qualicy_class) as qualicy_class,
       cy.actor is not null                         as is_active,
       coalesce(cy.year, ly.current_year + 1)       as currrent_year,
       case
           when ly.films is null then cy.films
           when cy.films is null then ly.films
           when cy.films is not null
               and ly.films is not null then cy.films || ly.films
           end                                      as films
from ly_data ly
         full outer join cy_data cy on ly.actor_id = cy.actor_id