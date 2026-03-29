{{
  config(
    materialized='view'
  )
}}

/*
  stg_recommendation_logs — date fix
  recommendation_date is INT64 nanoseconds in BQ.
*/

with source as (
    select * from {{ source('netflix_raw', 'recommendation_logs') }}
),

cleaned as (
    select
        recommendation_id as recommendation_id,
        user_id           as user_id,
        movie_id          as movie_id,

        date(timestamp_micros(cast(cast(recommendation_date as float64) / 1000 as int64)))  as recommendation_date,

        recommendation_type   as recommendation_type,
        cast(recommendation_score as float64)  as recommendation_score,
        cast(was_clicked       as bool)        as was_clicked,
        cast(position_in_list  as int64)       as position_in_list,
        device_type       as device_type,
        time_of_day       as time_of_day,
        algorithm_version as algorithm_version,

        case
            when cast(position_in_list as int64) <= 5  then 'top_5'
            when cast(position_in_list as int64) <= 10 then 'top_10'
            else 'below_10'
        end as position_bucket,

        safe.parse_date('%Y-%m-%d', _ingestion_date) as ingestion_date

    from source
    where recommendation_id is not null
      and user_id            is not null
      and movie_id           is not null
)

select * from cleaned