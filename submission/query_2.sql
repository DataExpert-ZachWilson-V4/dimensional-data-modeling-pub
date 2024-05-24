insert into actors --sequential loading of records based on year
		with last_year as( --reading previous year records from actors table
		select * 
		from actors
		where current_year=2000
		),
		this_year as( --reading current year records from actor_films table
		select *
		from bootcamp.actor_films
		where year=2001
		)
		select coalesce(l.actor,t.actor)as actor, --merge common columns
		coalesce(l.actor_id,t.actor_id)as actor_id,
		case when t.film is null then l.films --if no film array exists for actor in most recent year, keep film array from previous years
		when t.film is not null and l.films is null then --if film array exists for actor in most recent year and no film array exists for previous years, add a row for most recent year
		array[row(
		t.film,
		t.votes,
		t.rating,
		t.film_id
		)]
		when t.film is not null and l.films is not null then --if film array exists for actor in most recent year and film array exists for previous years, add a row for most recent year and append earlier array along with most recent array
		array[row(
		t.film,
		t.votes,
		t.rating,
		t.film_id
		)]||l.films end as films,
		case when avg(t.rating) over (partition by t.actor)>8 then 'star' --case statement to categorize average rating of films partitioned by actor
		when avg(t.rating) over (partition by t.actor)>7 and avg(t.rating) over (partition by t.actor)<=8 then 'good'
		when avg(t.rating) over (partition by t.actor)>6 and avg(t.rating) over (partition by t.actor)<=7 then 'average'
		when avg(t.rating)over (partition by t.actor)<=6 then 'bad'end as quality_class,
		t.year is not null as is_active, --boolean variable to check if actor is active in current year
		coalesce(t.year,l.current_year+1) as current_year
		from last_year l
		full outer join this_year t --full outer join previous year and current year records on actor key
		on l.actor_id=t.actor_id
                group by l.actor, t.actor, l.actor_id, t.actor_id, l.films, t.film, t.votes, t.rating, t.film_id, l.quality_class, t.year,l.current_year
