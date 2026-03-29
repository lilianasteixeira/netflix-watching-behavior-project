{{
  config(
    materialized='view'
  )
}}

/*
  stg_search_logs — date fix
  search_date is INT64 nanoseconds in BQ.
*/

with source as (
    select * from {{ source('netflix_raw', 'search_logs') }}
),

cleaned as (
    select
        search_id    as search_id,
        user_id      as user_id,
        search_query as search_query,

        date(timestamp_micros(cast(cast(search_date as float64) / 1000 as int64)))  as search_date,

        cast(results_returned        as int64)    as results_returned,
        cast(clicked_result_position as int64)    as clicked_result_position,
        device_type  as device_type,
        cast(search_duration_seconds as float64)  as search_duration_seconds,
        cast(had_typo    as bool)  as had_typo,
        cast(used_filters as bool) as used_filters,
        location_country as location_country,

        case
            when clicked_result_position is not null then true
            else false
        end as had_click,

        case
            when results_returned = 0                        then 'zero_results'
            when clicked_result_position is null             then 'no_click'
            when cast(clicked_result_position as int64) <= 3 then 'top_3_click'
            else 'lower_click'
        end as search_outcome,

        safe.parse_date('%Y-%m-%d', _ingestion_date) as ingestion_date

    from source
    where search_id is not null
      and user_id   is not null
)

select * from cleaned