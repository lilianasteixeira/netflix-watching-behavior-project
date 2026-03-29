{{
  config(
    materialized='view'
  )
}}

/*
  stg_movies — date fix
  Parquet datetime64[ns] columns land in BQ as INT64 (nanoseconds since epoch).
  Convert: DATE(TIMESTAMP_MICROS(CAST(col AS INT64) / 1000))
  _ingestion_date is already STRING '2026-03-28' — use SAFE.PARSE_DATE.
*/

with source as (
    select * from {{ source('netflix_raw', 'movies') }}
),

cleaned as (
    select
        movie_id          as movie_id,
        title             as title,
        content_type      as content_type,
        genre_primary     as genre_primary,
        genre_secondary   as genre_secondary,
        rating            as age_rating,
        language          as language,
        country_of_origin as country_of_origin,

        cast(release_year as int64)                                          as release_year,
        date(timestamp_micros(cast(cast(added_to_platform as float64) / 1000 as int64)))     as added_to_platform_date,

        cast(duration_minutes   as float64)  as duration_minutes,
        cast(imdb_rating        as float64)  as imdb_rating,
        cast(production_budget  as float64)  as production_budget,
        cast(box_office_revenue as float64)  as box_office_revenue,
        cast(number_of_seasons  as int64)    as number_of_seasons,
        cast(number_of_episodes as int64)    as number_of_episodes,
        cast(is_netflix_original as bool)    as is_netflix_original,
        cast(content_warning     as bool)    as has_content_warning,

        case
            when content_type in ('Movie', 'Stand-up Comedy', 'Documentary')
            then true else false
        end as is_single_title,

        safe.parse_date('%Y-%m-%d', _ingestion_date) as ingestion_date

    from source
    where movie_id is not null
)

select * from cleaned