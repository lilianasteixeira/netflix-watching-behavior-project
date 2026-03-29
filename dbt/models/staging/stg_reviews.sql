{{
  config(
    materialized='view'
  )
}}

/*
  stg_reviews — date fix
  review_date is INT64 nanoseconds in BQ.
*/

with source as (
    select * from {{ source('netflix_raw', 'reviews') }}
),

cleaned as (
    select
        review_id as review_id,
        user_id   as user_id,
        movie_id  as movie_id,

        cast(rating as int64)  as rating,

        date(timestamp_micros(cast(cast(review_date as float64) / 1000 as int64)))  as review_date,

        device_type as device_type,
        cast(is_verified_watch as bool)   as is_verified_watch,
        cast(helpful_votes     as int64)  as helpful_votes,
        cast(total_votes       as int64)  as total_votes,
        review_text  as review_text,
        sentiment    as sentiment,
        cast(sentiment_score as float64)  as sentiment_score,

        case
            when cast(total_votes as int64) > 0
            then round(
                safe_divide(
                    cast(helpful_votes as float64),
                    cast(total_votes   as float64)
                ), 4)
            else null
        end as helpfulness_ratio,

        case
            when cast(rating as int64) >= 4 then 'positive'
            when cast(rating as int64) = 3  then 'neutral'
            else 'negative'
        end as rating_sentiment,

        safe.parse_date('%Y-%m-%d', _ingestion_date) as ingestion_date

    from source
    where review_id is not null
      and user_id   is not null
      and movie_id  is not null
      and rating between 1 and 5
)

select * from cleaned