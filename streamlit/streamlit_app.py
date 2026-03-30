# STREAMLIT APP — SIMPLIFIED FILTERS + DATE FILTER + GENRE PIE

import os
import streamlit as st
import pandas as pd
import plotly.express as px
from google.cloud import bigquery
from google.oauth2 import service_account

st.set_page_config(page_title="Netflix Analytics", layout="wide")

PROJECT = os.getenv("GCP_PROJECT_ID", "netflix-user-behavior-490622")
DATASET = os.getenv("BQ_DATASET_MARTS", "netflix_marts")

@st.cache_resource
def get_client():
    creds = service_account.Credentials.from_service_account_file(
        os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "/opt/keys/gcp-sa.json")
    )
    return bigquery.Client(project=PROJECT, credentials=creds)

client = get_client()

@st.cache_data(ttl=600)
def q(sql):
    return client.query(sql).to_dataframe()

# ─────────────────────────────────────────────────────────────
# ✅ SIMPLE FILTERS + DATE FILTER
# ─────────────────────────────────────────────────────────────
with st.sidebar:
    st.title("Filters")

    plans = st.multiselect(
        "Subscription Plan",
        ["Basic","Standard","Premium","Premium+"],
        default=["Basic","Standard","Premium","Premium+"]
    )

    countries = st.multiselect(
        "Country",
        ["USA","Canada"],
        default=["USA","Canada"]
    )

    # 📅 DATE FILTER
    date_range = st.date_input(
        "Select Date Range",
        []
    )

# SAFE FILTER BUILDER

def build_filter(column, values):
    if not values:
        return ""
    formatted = ",".join([f"'{v}'" for v in values])
    return f"AND {column} IN ({formatted})"

plan_clause = build_filter("subscription_plan", plans)
country_clause = build_filter("country", countries)

# DATE CLAUSE
if len(date_range) == 2:
    start_date, end_date = date_range
    date_clause_users = f"AND last_watch_date BETWEEN '{start_date}' AND '{end_date}'"
    date_clause_content = f"AND watch_month BETWEEN '{start_date}' AND '{end_date}'"
else:
    date_clause_users = ""
    date_clause_content = ""

# ─────────────────────────────────────────────────────────────
# KPI
# ─────────────────────────────────────────────────────────────
kpi = q(f"""
SELECT
    COUNT(*) AS total_users,
    SAFE_DIVIDE(COUNTIF(is_active), COUNT(*))*100 AS active_rate,
    SAFE_DIVIDE(SUM(total_watch_minutes), COUNT(*))/60 AS avg_watch_hours
FROM `{PROJECT}.{DATASET}.mart_user_engagement`
WHERE 1=1
{plan_clause}
{country_clause}
{date_clause_users}
""")

k = kpi.iloc[0]
cols = st.columns(3)
cols[0].metric("Users", f"{int(k.total_users):,}")
cols[1].metric("Active %", f"{k.active_rate:.1f}%")
cols[2].metric("Avg Watch", f"{k.avg_watch_hours:.1f}h")

# ─────────────────────────────────────────────────────────────
# TABS
# ─────────────────────────────────────────────────────────────
tab1, tab2, tab3 = st.tabs(["Users","Content","Genres"])

# ─────────────────────────────────────────────────────────────
# USERS — TEMPORAL GRAPH
# ─────────────────────────────────────────────────────────────
with tab1:
    st.subheader("User Activity Over Time")

    df = q(f"""
    SELECT DATE_TRUNC(last_watch_date, MONTH) AS month,
           COUNT(*) AS users
    FROM `{PROJECT}.{DATASET}.mart_user_engagement`
    WHERE is_active = TRUE
    {plan_clause}
    {country_clause}
    {date_clause_users}
    GROUP BY 1
    ORDER BY 1
    """)

    df["month"] = pd.to_datetime(df["month"])
    fig = px.line(df, x="month", y="users", markers=True)
    st.plotly_chart(fig, use_container_width=True)

# ─────────────────────────────────────────────────────────────
# CONTENT
# ─────────────────────────────────────────────────────────────
with tab2:
    st.subheader("Content Completion vs Sessions")

    df = q(f"""
    SELECT
        genre_primary,
        SUM(total_sessions) AS sessions,
        AVG(completion_rate_pct) AS completion
    FROM `{PROJECT}.{DATASET}.mart_content_performance`
    WHERE 1=1
    {date_clause_content}
    GROUP BY 1
    """)

    fig = px.scatter(df, x="sessions", y="completion", size="sessions", color="genre_primary")
    st.plotly_chart(fig, use_container_width=True)

# ─────────────────────────────────────────────────────────────
# GENRES — PIE CHART (REQUESTED)
# ─────────────────────────────────────────────────────────────
with tab3:
    st.subheader("Genre Distribution (Sessions)")

    df = q(f"""
    SELECT
        genre_primary,
        SUM(sessions) AS sessions
    FROM `{PROJECT}.{DATASET}.mart_genre_trends`
    WHERE 1=1
    GROUP BY 1
    ORDER BY sessions DESC
    """)

    fig = px.pie(df, names="genre_primary", values="sessions", hole=0.4)
    st.plotly_chart(fig, use_container_width=True)
