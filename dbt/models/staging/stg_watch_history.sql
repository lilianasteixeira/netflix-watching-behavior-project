{{
  config(
    materialized='view'
  )
}}

/*
  stg_watch_history — date fix
  watch_date is INT64 nanoseconds in BQ.
*/

with source as (
    select * from {{ source('netflix_raw', 'watch_history') }}
),

cleaned as (
    select
        session_id as session_id,
        user_id    as user_id,
        movie_id   as movie_id,

        date(timestamp_micros(cast(cast(watch_date as float64) / 1000 as int64)))  as watch_date,

        device_type      as device_type,
        location_country as location_country,

        cast(watch_duration_minutes as float64)  as watch_duration_minutes,
        cast(progress_percentage    as float64)  as progress_percentage,
        action   as action,
        quality  as quality,
        cast(is_download  as bool)    as is_download,
        cast(user_rating  as float64) as user_rating,

        case
            when action = 'completed'
              or cast(progress_percentage as float64) >= 90
            then true
            else false
        end as is_completed,

        case
            when cast(watch_duration_minutes as float64) is null  then 'no_data'
            when cast(watch_duration_minutes as float64) < 10     then 'bounce'
            when cast(watch_duration_minutes as float64) < 30     then 'short'
            when cast(watch_duration_minutes as float64) < 90     then 'medium'
            else 'long'
        end as session_length_bucket,

        safe.parse_date('%Y-%m-%d', _ingestion_date) as ingestion_date

    from source
    where session_id is not null
      and user_id    is not null
      and movie_id   is not null
)

select * from cleaned