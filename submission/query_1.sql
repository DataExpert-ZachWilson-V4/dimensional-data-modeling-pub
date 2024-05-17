CREATE TABLE bgar.actors (
   actor VARCHAR,
   actor_id VARCHAR,
   films ARRAY(ROW(
     film VARCHAR,  -- 'films': Array of ROWs for multiple films associated with each actor. Each row contains film details.
     votes INTEGER,
     rating DOUBLE,
     film_id VARCHAR,
     year INTEGER
   )),
   quality_class VARCHAR,
   is_active BOOLEAN,   -- 'current_year': Represents the year this row is relevant for the actor.
   current_year INTEGER   -- 'current_year': Represents the year this row is relevant for the actor.
 )
 WITH (
   format = 'PARQUET',
   partitioning = ARRAY['current_year']   -- Partitioned by 'current_year' for efficient time-based analysis.
 )
