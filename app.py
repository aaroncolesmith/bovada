import pandas as pd
import requests
#from pandas import json_normalize
from pandas.io.json import json_normalize
import datetime
import io
from io import StringIO
import boto3
import streamlit as st
import numpy as np
import plotly_express as px
import geocoder
import SessionState

def max_minus_min(x):
    return max(x) - min(x)

def last_minus_avg(x):
    return last(x) - mean(x)

@st.cache
def get_s3_data(bucket, key):
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(io.BytesIO(obj['Body'].read()))
    return df

def save_to_s3(df, bucket, key):
    s3_resource = boto3.resource('s3')
    csv_buffer = StringIO()
    df.to_csv(csv_buffer,index=False)
    s3_resource.Object(bucket, key).put(Body=csv_buffer.getvalue())

def get_select_options(df, track_df):
    b=track_df.groupby(['selection']).agg({'count':'sum'}).sort_values(['count'], ascending=False).reset_index(drop=False)
    a=pd.merge(df, b, left_on='title',right_on='selection', how = 'left').sort_values(['count'], ascending=False)
    a=a['title'].unique()
    a=np.insert(a,0,'')
    return a

def table_output(df):
    st.write(df.groupby(['Winner']).agg({'Date':'max','Price': ['last','mean','max','min',max_minus_min,'count']}).sort_values([('Price', 'mean')], ascending=True))

def line_chart(df, option):
    g=px.line(df, x='Date', y='Price', color='Winner', title='Betting Odds Over Time')
    g.update_traces(mode='lines',opacity=.75,
                   line = dict(width=4))
    g.update_xaxes(title='')
    st.plotly_chart(g)

def line_chart_probability(df,option):
    g=px.line(df, x='Date', y='Implied_Probability', color='Winner', title='Implied Probabiilty Over Time')
    g.update_traces(mode='lines',opacity=.75,
                   line = dict(width=4))
    g.update_yaxes(range=[0, 1])
    g.update_xaxes(title='')
    st.plotly_chart(g)


def line_chart_favorites(df, option):
    #today = pd.Timestamp.today()
    #last_7 = today - timedelta(days=7)
    #g=px.line(df.loc[(df.Price < 5000) & (df.Date > last_7)], x='Date', y='Price', color='Winner', title='Betting Odds Over Time -- Recent & Favorites')
    g=px.line(df.loc[(df.Price < 5000)], x='Date', y='Price', color='Winner', title='Betting Odds Over Time -- Recent & Favorites')
    g.update_traces(mode='lines+markers')
    st.plotly_chart(g)

def scatter_plot(df, option):
    s=df.groupby(['Winner']).agg({'Date':'max','Price': ['last','mean','max','min','count']}).sort_values([('Price', 'mean')], ascending=True)
    s=s.reset_index(drop = False)
    s.columns=['Winner','Date','Last Price','Average','Max','Min','Count']
    g=px.scatter(s, x='Average',y='Last Price',color='Winner', title='Average Bet Price vs. Most Recent')
    st.plotly_chart(g)

def bovada_request(url, df):
    req = requests.get(url)
    for i in range(json_normalize(req.json()).index.size):
        for j in range(json_normalize(req.json()[i]['events']).index.size):
            for k in range(json_normalize(req.json()[i]['events'][j]['displayGroups']).index.size):
                for l in range(json_normalize(req.json()[i]['events'][j]['displayGroups'][k]['markets']).index.size):
                    d=json_normalize(req.json()[i]['events'][j]['displayGroups'][k]['markets'][l]['outcomes'])
                    a=json_normalize(req.json()[i]['path'])
                    d['group'] = a['description'].loc[0]
                    d['title'] = req.json()[i]['events'][j]['description'] + ' - ' + req.json()[i]['events'][j]['displayGroups'][k]['markets'][l]['description']
                    d['url'] = url
                    d['date'] = datetime.datetime.now()
                    try:
                        df = pd.concat([df, d], sort=False)
                    except:
                        df=d
    df['price.american']=df['price.american'].replace('EVEN',0)
    df['price.american']=df['price.american'].astype('int')
    df['date'] = pd.to_datetime(df['date'])
    return df

@st.cache
def update_bovada(df, url):
    for i in range(len(url)):
        df = bovada_request(url[i],df)
    bucket = 'bovada-scrape'
    df_file = 'bovada_requests.csv'
    save_to_s3(df, bucket, df_file)
    return df


def time_since_last_run(df):
    try:
        max_date = pd.to_datetime(df.date.max())
        now=pd.to_datetime(datetime.datetime.now())
        time_since = (now-max_date).total_seconds()
    except Exception:
            time_since = 86401
    return round(time_since,2)

def analytics(title):
    bucket = 'bovada-scrape'
    a_file = 'bovada_analytics.csv'
    g_df = get_s3_data(bucket,a_file)
    g_in_df = json_normalize(geocoder.ip('me').json)
    g_in_df['date'] = datetime.datetime.now()
    g_in_df['title'] = title
    g_df = pd.concat([g_df, g_in_df], sort=False)
    save_to_s3(g_df, bucket, a_file)

def _max_width_():
    max_width_str = f"max-width: 2000px;"
    st.markdown(
        f"""
    <style>
    .reportview-container .main .block-container{{
        {max_width_str}
    }}
    </style>
    """,
        unsafe_allow_html=True,
    )


def main():
    #Get all data
    #_max_width_()
    st.title('Bovada Odds Over Time')
    st.markdown('Welcome to Bovada Scrape!!! Select an option below and see how the betting odds have tracked over time!')
    bucket = 'bovada-scrape'
    df_file = 'bovada_requests.csv'
    track_file = 'track_df.csv'
    #analytics('Initialized')

    url = [
    'https://www.bovada.lv/services/sports/event/coupon/events/A/description/soccer?marketFilterId=rank&preMatchOnly=true&lang=en',
    'https://www.bovada.lv/services/sports/event/coupon/events/A/description/entertainment?marketFilterId=def&preMatchOnly=true&lang=en',
    'https://www.bovada.lv/services/sports/event/coupon/events/A/description/basketball?marketFilterId=rank&preMatchOnly=true&lang=en',
    'https://www.bovada.lv/services/sports/event/coupon/events/A/description/politics?marketFilterId=rank&preMatchOnly=true&lang=en',
    'https://www.bovada.lv/services/sports/event/coupon/events/A/description/football?marketFilterId=rank&preMatchOnly=true&lang=en',
    'https://www.bovada.lv/services/sports/event/coupon/events/A/description/baseball?marketFilterId=rank&preMatchOnly=true&lang=en',
    'https://www.bovada.lv/services/sports/event/coupon/events/A/description/boxing?marketFilterId=def&preMatchOnly=true&lang=en',
    'https://www.bovada.lv/services/sports/event/coupon/events/A/description/basketball/college-basketball?marketFilterId=rank&preMatchOnly=true&lang=en'
]

    df = get_s3_data(bucket,df_file)

    #df['date'] = pd.to_datetime(df['date'])

    t=time_since_last_run(df)

    if t > 10800:
        try:
            df = update_bovada(df, url)
        except:
            pass

    track_df = get_s3_data(bucket,track_file)

    # state = SessionState.get(option = '', a=[])
    #
    # state.a = get_select_options(df, track_df)
    #
    # state.option=st.selectbox('Select a bet -', state.a)
    a=get_select_options(df, track_df)
    option=st.selectbox('Select a bet -', a)

    if len(option) > 0:
        try:
            track_df = track_df.append({'date' : datetime.datetime.now()
                             ,'selection' :  option,
                             'count' : 1} , ignore_index=True)
            save_to_s3(track_df, bucket, track_file)
            #analytics(state.option)

            st.title(option)
            filtered_df = df.loc[df.title == option]
            filtered_df = filtered_df[['date','url','title','description','price.american']].reset_index(drop=True)
            filtered_df.columns = ['Date','URL','Title','Winner','Price']
            filtered_df.loc[filtered_df['Price'] > 0, 'Implied_Probability'] = 100 / (filtered_df['Price'] + 100)
            filtered_df.loc[filtered_df['Price'] < 0, 'Implied_Probability'] = -filtered_df['Price'] / (-filtered_df['Price'] + 100)
            filtered_df['Date'] = pd.to_datetime(filtered_df['Date'])

            table_output(filtered_df)
            line_chart(filtered_df,option)
            line_chart_probability(filtered_df,option)

            # if filtered_df.Price.max() >= 7500:
            #         line_chart_favorites(filtered_df,option)
            #
            # scatter_plot(filtered_df, option)

        except:
            st.markdown('Select an option above')

if __name__ == "__main__":
    #execute
    main()
