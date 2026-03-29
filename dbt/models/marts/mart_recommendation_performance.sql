{{
  config(
    materialized='table',
    partition_by={
      "field": "recommendation_month",
      "data_type": "date",
      "granularity": "month"
    },
    cluster_by=["recommendation_type", "algorithm_version"]
  )
}}

/*
  mart_recommendation_performance
  ────────────────────────────────
  One row per (recommendation_type, algorithm_version, month).
  Answers:
    - Which algorithm version has the best click-through rate?
    - Does position in list dramatically affect CTR?
    - Which recommendation type converts best?
    - Does time of day affect click probability?
    - When a rec is clicked — does the user actually watch the content?
*/

with recs as (
    select * from {{ ref('stg_recommendation_logs') }}
),

watch as (
    select
        user_id,
        movie_id,
        watch_date,
        is_completed,
        watch_duration_minutes
    from {{ ref('stg_watch_history') }}
),

-- Join: did a recommendation lead to a watch session within 7 days?
rec_with_conversion as (
    select
        r.recommendation_id,
        r.user_id,
        r.movie_id,
        r.recommendation_date,
        r.recommendation_type,
        r.recommendation_score,
        r.was_clicked,
        r.position_in_list,
        r.position_bucket,
        r.device_type,
        r.time_of_day,
        r.algorithm_version,
        r.ingestion_date,

        -- conversion: any watch session for that (user, movie) within 7 days of rec
        max(case
            when w.user_id  = r.user_id
             and w.movie_id = r.movie_id
             and w.watch_date between r.recommendation_date
                                  and date_add(r.recommendation_date, interval 7 day)
            then true else false
        end) over (partition by r.recommendation_id) as led_to_watch,

        max(case
            when w.user_id  = r.user_id
             and w.movie_id = r.movie_id
             and w.watch_date between r.recommendation_date
                                  and date_add(r.recommendation_date, interval 7 day)
             and w.is_completed
            then true else false
        end) over (partition by r.recommendation_id) as led_to_completion

    from recs r
    left join watch w
        on  w.user_id  = r.user_id
        and w.movie_id = r.movie_id
        and w.watch_date between r.recommendation_date
                              and date_add(r.recommendation_date, interval 7 day)
),

deduped as (
    select distinct * from rec_with_conversion
),

monthly as (
    select
        recommendation_type,
        coalesce(algorithm_version, 'unknown')  as algorithm_version,
        device_type,
        time_of_day,
        position_bucket,
        date_trunc(recommendation_date, month)  as recommendation_month,

        count(*)                                as total_recommendations,
        count(distinct user_id)                 as unique_users_targeted,
        count(distinct movie_id)                as unique_titles_recommended,

        -- click-through
        countif(was_clicked)                    as total_clicks,
        round(
            safe_divide(countif(was_clicked), count(*)) * 100, 2
        )                                       as ctr_pct,

        -- downstream conversion
        countif(led_to_watch)                   as watch_conversions,
        round(
            safe_divide(countif(led_to_watch), count(*)) * 100, 2
        )                                       as watch_conversion_pct,

        countif(led_to_completion)              as completion_conversions,
        round(
            safe_divide(countif(led_to_completion), count(*)) * 100, 2
        )                                       as completion_conversion_pct,

        -- score quality (where available)
        round(avg(recommendation_score), 3)     as avg_recommendation_score,

        -- position effectiveness
        countif(position_in_list = 1)           as recs_in_position_1,
        countif(position_in_list <= 5 and was_clicked) as clicks_top_5,
        countif(position_in_list > 5  and was_clicked) as clicks_below_5

    from deduped
    group by 1, 2, 3, 4, 5, 6
)

select * from monthly
