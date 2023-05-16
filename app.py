import streamlit as st
import pandas as pd
import altair as alt
import numpy as np
import snowflake.connector
from pytrends.request import TrendReq

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

@st.cache_data
def add_pytrend(kw_list, kw_ref):
    pytrends = TrendReq(hl='en-US', tz=360)
    #pytrends = TrendReq(hl='en-US', tz=360, proxies=['https://34.203.233.13:80',], retries=2, backoff_factor=0.1, requests_args={'verify':False})

    df = pd.DataFrame(columns=['name', 't_date', 'interest_static', 'interest_relative', 'reference_interest'])

    for kw in kw_list:
        pytrends.build_payload([kw], timeframe='today 5-y')
        static = pytrends.interest_over_time()

        if static.empty:
            continue

        pytrends.build_payload([kw, kw_ref], timeframe='today 5-y')
        relative = pytrends.interest_over_time()

        out = pd.merge(relative, static, on=['date'], suffixes=['_multi', '_single'])\
            .filter(items=[kw + '_multi', kw_ref, kw + '_single'])\
            .reset_index()\
            .rename(columns={'date': 't_date', kw + '_multi': 'interest_relative', kw + '_single': 'interest_static', kw_ref: 'reference_interest'})
        
        #out['t_date'] = pd.to_datetime(out['t_date']).strftime("%Y/%m/%d %H:%M:%S")
        out['t_date'] = pd.to_datetime(out['t_date']).dt.tz_localize('UTC')
        out['name'] = kw
        out = out[['name', 't_date', 'interest_static', 'interest_relative', 'reference_interest']]
        df = pd.concat([df, out], ignore_index=True)
        
    return df

cnx = sf_connect()
data = load_data(cnx)

st.title("AI Tool Trends")

extra = st.text_input('Add another ai tool')
if extra != '':
    new_df = add_pytrend([extra], 'ai tool')
    new_df['t_date'] = pd.to_datetime(new_df['t_date']).dt.tz_convert(None)
    data = pd.concat([data, new_df], ignore_index=True)

mask = data['reference_interest'] < 0.1
data.loc[mask, 'reference_interest'] = 0.1

names = data['name'].unique()

options = st.multiselect('AI Tools', names, ['Jasper.ai', 'Anyword', 'Pictory'])
chart_data = data.assign(popularity = data['interest_static'].astype(np.uint64) * data['interest_relative'].astype(np.uint64) / data['reference_interest'].astype(np.uint64))
chart_data = chart_data[chart_data['t_date'] >= pd.to_datetime('2020-06-01 00:00:00')] #pd.Timestamp('2020-06-01').tz_localize('US/Eastern')
rank = chart_data[['name', 'popularity']]
mask = chart_data['name'].isin(options)
chart_data = chart_data.loc[mask]


rank = rank.groupby('name').idxmax()
rank.sort_values(by=['popularity'], inplace=True, ascending=False)
st.sidebar.title('Ranking')
st.sidebar.table(rank)


chart = alt.Chart(chart_data).mark_line().encode(
    alt.X('t_date', title='Date'),
    alt.Y('popularity', title='Popularity'),
    color='name'
    ).properties(
    width=1200,
    height=500
    )

st.altair_chart(chart)
