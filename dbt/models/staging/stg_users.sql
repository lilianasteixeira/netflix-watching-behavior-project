{{
  config(
    materialized='view'
  )
}}

/*
  stg_users — date fix
  subscription_start_date and created_at are INT64 nanoseconds in BQ.
*/

with source as (
    select * from {{ source('netflix_raw', 'users') }}
),

cleaned as (
    select
        user_id           as user_id,
        safe_cast(age as float64)  as age,
        gender            as gender,
        country           as country,
        state_province    as state_province,
        city              as city,
        subscription_plan as subscription_plan,

        date(timestamp_micros(cast(cast(subscription_start_date as float64) / 1000 as int64)))  as subscription_start_date,
        cast(is_active as bool)              as is_active,
        cast(monthly_spend  as float64)      as monthly_spend,
        primary_device      as primary_device,
        cast(household_size as int64)        as household_size,

        timestamp_micros(cast(cast(created_at as float64) / 1000 as int64))  as created_at,

        safe.parse_date('%Y-%m-%d', _ingestion_date)  as ingestion_date,

        case subscription_plan
            when 'Basic'    then 1
            when 'Standard' then 2
            when 'Premium'  then 3
            when 'Premium+' then 4
            else 0
        end as plan_tier_rank,

        case
            when safe_cast(age as float64) < 25              then 'Gen Z (< 25)'
            when safe_cast(age as float64) between 25 and 34 then 'Millennial (25–34)'
            when safe_cast(age as float64) between 35 and 49 then 'Gen X (35–49)'
            when safe_cast(age as float64) >= 50             then 'Boomer+ (50+)'
            else 'Unknown'
        end as age_bucket

    from source
    where user_id is not null
)

select * from cleaned