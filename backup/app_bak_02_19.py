import pandas as pd
import requests
from pandas.io.json import json_normalize
import datetime
import io
from io import StringIO
import boto3
import streamlit as st
import numpy as np
import plotly_express as px
import SessionState

def load_data():
    st.sidebar.markdown('Just ran load_data')
    df = get_df_s3()
    return df


def load_track_data():
    s3 = boto3.client('s3')
    bucket = 'bovada-scrape'
    key = 'track_df.csv'
    obj = s3.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(io.BytesIO(obj['Body'].read()))
    return df


def max_minus_min(x):
    return max(x) - min(x)


def last_minus_avg(x):
    return last(x) - mean(x)


def save_to_s3(df):
    bucket = 'bovada-scrape'
    csv_buffer = StringIO()
    df.to_csv(csv_buffer,index=False)
    s3_resource = boto3.resource('s3')
    filename = 'bovada_requests.csv'
    s3_resource.Object(bucket, filename).put(Body=csv_buffer.getvalue())


def get_df_s3():
    s3 = boto3.client('s3')
    bucket = 'bovada-scrape'
    key = 'bovada_requests.csv'
    obj = s3.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(io.BytesIO(obj['Body'].read()))
    return df


def save_track(track_df):
    bucket = 'bovada-scrape'
    csv_buffer = StringIO()
    track_df.to_csv(csv_buffer,index=False)
    s3_resource = boto3.resource('s3')
    filename = 'track_df.csv'
    s3_resource.Object(bucket, filename).put(Body=csv_buffer.getvalue())


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
    save_to_s3(df)
    return df


def time_since_last_run(df, option):
    try:
        df = df.loc[df.title == option]
        max_date = pd.to_datetime(df.date.max())
        now=pd.to_datetime(datetime.datetime.now())
        time_since = (now-max_date).total_seconds()
    except Exception:
            time_since = 86401
    return round(time_since,2)

def table_output(df):
    #df.dtypes
    st.write(df.groupby(['Winner']).agg({'Date':'max','Price': ['last','mean','max','min',max_minus_min,'count']}).sort_values([('Price', 'mean')], ascending=True))


def line_chart(df, option):
    g=px.line(df, x='Date', y='Price', color='Winner', title=option+' Betting Odds Over Time')
    g.update_traces(mode='lines+markers')
    st.plotly_chart(g)


def line_chart_favorites(df, option):
    g=px.line(df.loc[df.Price < 5000], x='Date', y='Price', color='Winner', title=option+' Betting Odds Over Time')
    g.update_traces(mode='lines+markers')
    st.plotly_chart(g)


def draw_main(df, option, track_df):
    track_df = track_df.append({'date' : datetime.datetime.now()
                     ,'selection' :  option,
                     'count' : 1} , ignore_index=True)
    save_track(track_df)

    filtered_df = df.loc[df.title == option]
    filtered_df = filtered_df[['date','url','title','description','price.american']].reset_index(drop=True)
    filtered_df.columns = ['Date','URL','Title','Winner','Price']

    t=time_since_last_run(df, option)
    #st.sidebar.markdown('T is currently' + str(t))
    #st.sidebar.markdown('Last time you checked Bovada was ' + str(round(t/60,0)) + ' minutes ago.')
    if t > 500:
        url = filtered_df['URL'].iloc[-1]
        df = bovada_request(url, df)
        #st.sidebar.markdown('Just scraped ' + url)

    filtered_df = df.loc[df.title == option]
    filtered_df = filtered_df[['date','url','title','description','price.american']].reset_index(drop=True)
    filtered_df.columns = ['Date','URL','Title','Winner','Price']
    filtered_df['Date'] = pd.to_datetime(filtered_df['Date'])

    #st.write(filtered_df)
    table_output(filtered_df)
    line_chart(filtered_df,option)

    if filtered_df.Price.max() >= 7500:
            line_chart_favorites(filtered_df,option)


@st.cache
def get_select_options(df, track_df):
    b=track_df.groupby(['selection']).agg({'count':'sum'}).sort_values(['count'], ascending=False).reset_index(drop=False)
    a=pd.merge(df, b, left_on='title',right_on='selection', how = 'left').sort_values(['count'], ascending=False)
    a=a['title'].unique()
    a=np.insert(a,0,'')
    return a

def main():
    #_max_width_()
    df = load_data()

    track_df = load_track_data()

    state = SessionState.get(option = '', a=[])

    state.a = get_select_options(df, track_df)

    st.sidebar.markdown('Welcome to Bovada Scrape!!! Select an option below and see a visualization to the right!')
    #option=st.sidebar.selectbox('Select a bet -', a)

    state.option
    state.option=st.sidebar.selectbox('Select a bet -', state.a)
    st.sidebar.markdown('you selected ' + state.option)

    if len(state.option) > 0:
        try:
            draw_main(df,state.option,track_df)
        except:
            st.sidebar.markdown('Select an option above')


if __name__ == "__main__":
    #execute
    st.sidebar.markdown('Im in the main' + str(datetime.datetime.now()))
    main()
