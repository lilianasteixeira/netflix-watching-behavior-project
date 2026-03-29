{{
  config(
    materialized='table',
    partition_by={
      "field": "watch_month",
      "data_type": "date",
      "granularity": "month"
    },
    cluster_by=["content_type", "genre_primary"]
  )
}}

/*
  mart_content_performance
  ─────────────────────────
  One row per (movie_id, month).
  Joins watch_history + movies + reviews to answer:
    - What content drives the most watch time?
    - Which titles have the highest completion rates?
    - How do IMDB ratings compare to actual user behaviour?
    - What's the review sentiment breakdown per title per month?
*/

with watch as (
    select * from {{ ref('stg_watch_history') }}
    where watch_date is not null
),

movies as (
    select * from {{ ref('stg_movies') }}
),

reviews as (
    select * from {{ ref('stg_reviews') }}
),

-- Monthly watch aggregation per title
watch_monthly as (
    select
        movie_id,
        date_trunc(watch_date, month)           as watch_month,

        count(*)                                as total_sessions,
        count(distinct user_id)                 as unique_viewers,

        -- duration (null-safe)
        round(sum(watch_duration_minutes), 1)   as total_watch_minutes,
        round(avg(watch_duration_minutes), 1)   as avg_watch_minutes,

        -- completion
        countif(is_completed)                   as completed_sessions,
        round(
            safe_divide(countif(is_completed), count(*)) * 100, 1
        )                                       as completion_rate_pct,

        -- progress
        round(avg(progress_percentage), 1)      as avg_progress_pct,

        -- in-session ratings (80 % null — use as supplement only)
        round(avg(user_rating), 2)              as avg_inline_rating,

        -- quality split
        countif(quality = '4K')                 as sessions_4k,
        countif(quality = 'Ultra HD')           as sessions_ultra_hd,
        countif(quality = 'HD')                 as sessions_hd,
        countif(quality = 'SD')                 as sessions_sd,

        -- device split
        countif(device_type = 'Smart TV')       as sessions_smart_tv,
        countif(device_type = 'Mobile')         as sessions_mobile,
        countif(device_type = 'Laptop')         as sessions_laptop,
        countif(device_type = 'Desktop')        as sessions_desktop,
        countif(device_type = 'Tablet')         as sessions_tablet,

        -- download behaviour
        countif(is_download)                    as download_sessions

    from watch
    group by 1, 2
),

-- All-time review aggregation per title (reviews don't have month — join at title level)
review_summary as (
    select
        movie_id,
        count(*)                                as total_reviews,
        round(avg(rating), 2)                   as avg_review_rating,
        countif(sentiment = 'positive')         as positive_reviews,
        countif(sentiment = 'neutral')          as neutral_reviews,
        countif(sentiment = 'negative')         as negative_reviews,
        round(avg(sentiment_score), 3)          as avg_sentiment_score,
        countif(is_verified_watch)              as verified_reviews
    from reviews
    group by 1
)

select
    -- dimensions from movie catalogue
    m.movie_id,
    m.title,
    m.content_type,
    m.genre_primary,
    m.genre_secondary,
    m.age_rating,
    m.language,
    m.country_of_origin,
    m.release_year,
    m.duration_minutes                          as catalogue_duration_minutes,
    m.imdb_rating,
    m.is_netflix_original,
    m.has_content_warning,
    m.is_single_title,
    m.number_of_seasons,
    m.number_of_episodes,

    -- monthly watch grain
    w.watch_month,
    w.total_sessions,
    w.unique_viewers,
    w.total_watch_minutes,
    w.avg_watch_minutes,
    w.completed_sessions,
    w.completion_rate_pct,
    w.avg_progress_pct,
    w.avg_inline_rating,
    w.sessions_4k,
    w.sessions_ultra_hd,
    w.sessions_hd,
    w.sessions_sd,
    w.sessions_smart_tv,
    w.sessions_mobile,
    w.sessions_laptop,
    w.sessions_desktop,
    w.sessions_tablet,
    w.download_sessions,

    -- review signals (all-time, repeated across months — useful for filtering)
    coalesce(r.total_reviews, 0)                as total_reviews,
    r.avg_review_rating,
    coalesce(r.positive_reviews, 0)             as positive_reviews,
    coalesce(r.neutral_reviews, 0)              as neutral_reviews,
    coalesce(r.negative_reviews, 0)             as negative_reviews,
    r.avg_sentiment_score,
    coalesce(r.verified_reviews, 0)             as verified_reviews,

    -- composite engagement score (0–100)
    -- weights: watch volume 35%, completion 30%, avg progress 20%, review rating 15%
    round(
        coalesce(
            (w.total_sessions / nullif(max(w.total_sessions) over (partition by w.watch_month), 0))
            * 35, 0
        )
        + coalesce(w.completion_rate_pct / 100 * 30, 0)
        + coalesce(w.avg_progress_pct    / 100 * 20, 0)
        + coalesce(r.avg_review_rating   / 5.0 * 15, 0),
        2
    )                                           as engagement_score

from movies m
inner join watch_monthly w using (movie_id)
left join  review_summary r using (movie_id)
