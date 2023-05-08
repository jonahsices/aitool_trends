import streamlit as st
import pandas as pd
import altair as alt
import numpy as np
import snowflake.connector

@st.cache_resource
def sf_connect():
    cnx = snowflake.connector.connect(
        user=st.secrets["user"], 
        password=st.secrets["password"],
        account=st.secrets["account"],
        role=st.secrets["role"],
        warehouse=st.secrets["warehouse"],
        database=st.secrets["database"],
        schema=st.secrets["schema"],
        )
    return cnx

@st.cache_data
def load_data(_connection):
    cur = _connection.cursor()
    cur.execute('select * from ai_trends')
    df = cur.fetch_pandas_all()
    return df

cnx = sf_connect()
df = load_data(cnx)

mask = df['reference_interest'] < 0.1
df.loc[mask, 'reference_interest'] = 0.1

names = df['name'].unique()

st.title("Streamlit x Snowflake Hackathon")
options = st.multiselect('AI Tools', names, 'Canva')

chart_data = df.assign(popularity = df['interest_static'].astype(np.uint64) * df['interest_relative'].astype(np.uint64) / df['reference_interest'].astype(np.uint64))
chart_data = chart_data[chart_data['t_date'] >= '2021-01-01']
mask = chart_data['name'].isin(options)
chart_data = chart_data.loc[mask]

chart = alt.Chart(chart_data).mark_line().encode(
    alt.X('t_date', title='Date'),
    alt.Y('popularity', title='Popularity'),
    color='name'
    ).properties(
    width=1200,
    height=500
    )

st.altair_chart(chart)
