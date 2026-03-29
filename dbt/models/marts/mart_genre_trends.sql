{{
  config(
    materialized='table',
    partition_by={
      "field": "watch_week",
      "data_type": "date",
      "granularity": "month"
    },
    cluster_by=["genre_primary", "content_type"]
  )
}}

/*
  mart_genre_trends
  ──────────────────
  Weekly genre-level time series joining watch_history + movies.
  Enriched with review sentiment per genre per week.
  Answers:
    - Which genres are trending up or down week-over-week?
    - Which genres have the best completion rates?
    - How does review sentiment track with viewing volume?
    - Netflix Originals vs licensed — which genres perform better per type?
*/

with watch as (
    select * from {{ ref('stg_watch_history') }}
    where watch_date is not null
),

movies as (
    select
        movie_id,
        content_type,
        genre_primary,
        genre_secondary,
        is_netflix_original,
        imdb_rating
    from {{ ref('stg_movies') }}
),

reviews as (
    select
        movie_id,
        review_date,
        rating,
        sentiment
    from {{ ref('stg_reviews') }}
),

-- Enrich sessions with genre from movies catalogue
watch_enriched as (
    select
        w.session_id,
        w.user_id,
        w.watch_date,
        w.watch_duration_minutes,
        w.is_completed,
        w.progress_percentage,
        w.device_type,
        w.location_country,
        m.content_type,
        m.genre_primary,
        m.is_netflix_original
    from watch w
    inner join movies m using (movie_id)
),

weekly_watch as (
    select
        genre_primary,
        content_type,
        is_netflix_original,
        date_trunc(watch_date, week(monday))    as watch_week,

        count(*)                                as sessions,
        count(distinct user_id)                 as unique_viewers,
        round(sum(watch_duration_minutes), 1)   as total_watch_minutes,
        round(avg(watch_duration_minutes), 1)   as avg_session_minutes,
        countif(is_completed)                   as completed_sessions,
        round(
            safe_divide(countif(is_completed), count(*)) * 100, 1
        )                                       as completion_rate_pct,
        round(avg(progress_percentage), 1)      as avg_progress_pct,

        -- device breakdown
        countif(device_type = 'Smart TV')       as sessions_smart_tv,
        countif(device_type = 'Mobile')         as sessions_mobile,
        countif(device_type = 'Laptop')         as sessions_laptop,
        countif(device_type = 'Desktop')        as sessions_desktop,
        countif(device_type = 'Tablet')         as sessions_tablet,

        -- geo
        countif(location_country = 'USA')       as sessions_usa,
        countif(location_country = 'Canada')    as sessions_canada

    from watch_enriched
    group by 1, 2, 3, 4
),

-- Weekly review sentiment per genre (approximate — join on movie then aggregate by week)
weekly_reviews as (
    select
        m.genre_primary,
        date_trunc(r.review_date, week(monday)) as review_week,
        count(*)                                as review_count,
        round(avg(r.rating), 2)                 as avg_review_rating,
        countif(r.sentiment = 'positive')       as positive_reviews,
        countif(r.sentiment = 'negative')       as negative_reviews
    from reviews r
    inner join movies m using (movie_id)
    group by 1, 2
),

with_growth as (
    select
        w.*,

        -- Week-over-week session growth
        lag(w.sessions) over (
            partition by w.genre_primary, w.content_type, w.is_netflix_original
            order by w.watch_week
        )                                       as prev_week_sessions,

        round(
            safe_divide(
                w.sessions - lag(w.sessions) over (
                    partition by w.genre_primary, w.content_type, w.is_netflix_original
                    order by w.watch_week
                ),
                nullif(lag(w.sessions) over (
                    partition by w.genre_primary, w.content_type, w.is_netflix_original
                    order by w.watch_week
                ), 0)
            ) * 100, 2
        )                                       as wow_session_growth_pct,

        -- 4-week rolling average sessions
        round(avg(w.sessions) over (
            partition by w.genre_primary, w.content_type, w.is_netflix_original
            order by w.watch_week
            rows between 3 preceding and current row
        ), 1)                                   as sessions_4wk_avg,

        -- review signals for the same week/genre
        r.review_count,
        r.avg_review_rating,
        r.positive_reviews,
        r.negative_reviews

    from weekly_watch w
    left join weekly_reviews r
        on  r.genre_primary = w.genre_primary
        and r.review_week   = w.watch_week
)

select * from with_growth