import streamlit as st
from google.cloud import bigquery

client = bigquery.Client()

st.title("Netflix Analytics Dashboard")

@st.cache_data(ttl=300)
def load_data():
    query = """
        SELECT *
        FROM `netflix-user-behavior-490622.netflix_marts.user_engagement_summary`
    """
    return client.query(query).to_dataframe()

df = load_data()

st.metric("Total Users", len(df))
st.bar_chart(df["watch_time"])