INSERT INTO jessiii.actors
with
    -- Retrieves all actor data from the previous year (2000) to compare and update
    -- records for the current year.
    last_year as (select * from jessiii.actors where current_year = 2000),

    -- Selects all film entries from the current year (2001) to be aggregated and
    -- added to the actors table.
    this_year as (select * from bootcamp.actor_films where year = 2001),

    -- Aggregates film data for each actor from the current year, grouping by actor
    -- and calculating the total ratings and the distinct count of films.
    yearly_filmography as (
        select
            actor,
            actor_id,
            year,
            -- Creates an array of film data for each actor
            array_agg(row(film, votes, rating, film_id, year)) as films,
            -- Estimates the count of unique films per actor.
            approx_distinct(film_id) as number_of_films,
            -- Sums up all the ratings for films associated with an actor
            sum(rating) as sum_of_ratings
        from this_year
        group by actor, actor_id, year
    ),

    -- Calculates the average rating for films associated with each actor for the
    -- current year.
    average_rating as (
        select
            *,
            case
                when coalesce(number_of_films, 0) > 0  -- Checks if there are any films; if not, returns NULL for average.
                -- Computes the average rating.
                then round(sum_of_ratings / number_of_films, 2)
            end as average_rating
        from yearly_filmography
    )

-- Final SELECT to update or insert records into the actors table, merging last year
-- and this year's data.
select
    -- Coalesces actor names to handle potential missing values between years.
    coalesce(ly.actor, ty.actor) as actor,
    -- Ensures a continuous actor_id across years.
    coalesce(ly.actor_id, ty.actor_id) as actor_id,
    -- Bring previous year's filmography forward.
    case
        when ty.films is null
        then ly.films
        when ty.films is not null and ly.films is null
        then ty.films
        when ty.films is not null and ly.films is not null
        then ty.films || ly.films
    end as films,
    -- Determines the quality class based on the average ratin
    -- for the most recent year; carries forward if no new films.
    case
        when ty.films is null
        then ly.quality_class
        when average_rating > 8
        then 'star'
        when average_rating > 7 and average_rating <= 8
        then 'good'
        when average_rating > 6 and average_rating <= 7
        then 'average'
        when average_rating <= 6
        then 'bad'
    end as quality_class,
    -- Marks the actor as active if there are new films this year.
    ty.films is not null as is_active,
    coalesce(ty.year, ly.current_year + 1) as current_year  -- Updates the current year, incrementing from the last known year if no new data.
from last_year as ly
-- Joins last year's and this year's data on actor_id.
full outer join average_rating as ty on ly.actor_id = ty.actor_id

