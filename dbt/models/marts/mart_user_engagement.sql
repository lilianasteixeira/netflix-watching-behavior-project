{{
  config(
    materialized='table',
    cluster_by=["country", "subscription_plan", "engagement_tier"]
  )
}}

/*
  mart_user_engagement
  ─────────────────────
  One row per user — lifetime aggregation across all activity signals.
  Joins users + watch_history + reviews + search_logs + recommendation_logs.
  Answers:
    - Who are the power users vs churned subscribers?
    - Which subscription tiers generate the most watch time?
    - How does device preference correlate with engagement?
    - Which users are at churn risk?
*/

with users as (
    select * from {{ ref('stg_users') }}
),

watch as (
    select * from {{ ref('stg_watch_history') }}
),

reviews as (
    select * from {{ ref('stg_reviews') }}
),

searches as (
    select * from {{ ref('stg_search_logs') }}
),

recs as (
    select * from {{ ref('stg_recommendation_logs') }}
),

-- ── Per-user watch aggregation ──────────────────────────────────────────────
user_watch as (
    select
        user_id,
        count(*)                                    as total_sessions,
        count(distinct movie_id)                    as unique_titles_watched,
        count(distinct watch_date)                  as active_watch_days,
        min(watch_date)                             as first_watch_date,
        max(watch_date)                             as last_watch_date,

        round(sum(watch_duration_minutes), 1)       as total_watch_minutes,
        round(avg(watch_duration_minutes), 1)       as avg_session_minutes,
        round(avg(progress_percentage), 1)          as avg_progress_pct,

        countif(is_completed)                       as completed_sessions,
        round(safe_divide(countif(is_completed), count(*)) * 100, 1)
                                                    as completion_rate_pct,
        countif(is_download)                        as download_sessions,

        -- preferred device (mode approximation)
        approx_top_count(device_type, 1)[offset(0)].value  as preferred_device,
        approx_top_count(quality,     1)[offset(0)].value  as preferred_quality,
        approx_top_count(location_country, 1)[offset(0)].value as watch_country

    from watch
    group by 1
),

-- ── Per-user review aggregation ─────────────────────────────────────────────
user_reviews as (
    select
        user_id,
        count(*)                                as total_reviews,
        round(avg(rating), 2)                   as avg_rating_given,
        countif(sentiment = 'positive')         as positive_reviews,
        countif(sentiment = 'negative')         as negative_reviews,
        countif(is_verified_watch)              as verified_reviews
    from reviews
    group by 1
),

-- ── Per-user search aggregation ─────────────────────────────────────────────
user_searches as (
    select
        user_id,
        count(*)                                as total_searches,
        countif(had_click)                      as searches_with_click,
        countif(search_outcome = 'zero_results') as zero_result_searches,
        countif(had_typo)                       as searches_with_typo,
        round(avg(search_duration_seconds), 1)  as avg_search_duration_secs
    from searches
    group by 1
),

-- ── Per-user recommendation aggregation ─────────────────────────────────────
user_recs as (
    select
        user_id,
        count(*)                                as total_recommendations,
        countif(was_clicked)                    as recs_clicked,
        round(
            safe_divide(countif(was_clicked), count(*)) * 100, 1
        )                                       as rec_ctr_pct,
        approx_top_count(recommendation_type, 1)[offset(0)].value
                                                as most_common_rec_type
    from recs
    group by 1
)

select
    -- user profile (no PII)
    u.user_id,
    u.country,
    u.state_province,
    u.subscription_plan,
    u.plan_tier_rank,
    u.is_active,
    u.monthly_spend,
    u.primary_device,
    u.household_size,
    u.age,
    u.age_bucket,
    u.gender,
    u.subscription_start_date,
    u.created_at,

    -- watch behaviour
    coalesce(w.total_sessions,         0)       as total_sessions,
    coalesce(w.unique_titles_watched,  0)       as unique_titles_watched,
    coalesce(w.active_watch_days,      0)       as active_watch_days,
    w.first_watch_date,
    w.last_watch_date,
    coalesce(w.total_watch_minutes,    0)       as total_watch_minutes,
    w.avg_session_minutes,
    w.avg_progress_pct,
    coalesce(w.completed_sessions,     0)       as completed_sessions,
    w.completion_rate_pct,
    coalesce(w.download_sessions,      0)       as download_sessions,
    w.preferred_device,
    w.preferred_quality,

    -- review behaviour
    coalesce(r.total_reviews,          0)       as total_reviews,
    r.avg_rating_given,
    coalesce(r.positive_reviews,       0)       as positive_reviews,
    coalesce(r.negative_reviews,       0)       as negative_reviews,

    -- search behaviour
    coalesce(s.total_searches,         0)       as total_searches,
    coalesce(s.searches_with_click,    0)       as searches_with_click,
    coalesce(s.zero_result_searches,   0)       as zero_result_searches,

    -- recommendation behaviour
    coalesce(rc.total_recommendations, 0)       as total_recommendations,
    coalesce(rc.recs_clicked,          0)       as recs_clicked,
    rc.rec_ctr_pct,
    rc.most_common_rec_type,

    -- ── Derived segments ────────────────────────────────────────────────────

    -- Engagement tier based on total watch minutes
    case
        when coalesce(w.total_watch_minutes, 0) >= 3000 then 'power_viewer'
        when coalesce(w.total_watch_minutes, 0) >= 1000 then 'regular_viewer'
        when coalesce(w.total_watch_minutes, 0) >= 200  then 'casual_viewer'
        when coalesce(w.total_watch_minutes, 0) > 0     then 'light_viewer'
        else 'inactive'
    end as engagement_tier,

    -- Churn risk: inactive account OR no watch in last 30 days of dataset window
    case
        when not u.is_active then true
        when w.last_watch_date is null then true
        when date_diff(
            (select max(watch_date) from watch),
            w.last_watch_date,
            day
        ) > 30 then true
        else false
    end as is_churn_risk,

    -- Days since last watch (relative to dataset max date)
    date_diff(
        (select max(watch_date) from watch),
        w.last_watch_date,
        day
    ) as days_since_last_watch,

    -- Subscription tenure in days
    date_diff(
        current_date(),
        u.subscription_start_date,
        day
    ) as subscription_tenure_days

from users u
left join user_watch   w  using (user_id)
left join user_reviews r  using (user_id)
left join user_searches s using (user_id)
left join user_recs     rc using (user_id)
