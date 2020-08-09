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
import sys
# import geocoder

def max_minus_min(x):
    return max(x) - min(x)

def last_minus_avg(x):
    return last(x) - mean(x)

@st.cache(suppress_st_warning=True)
def get_s3_data(bucket, key):
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(io.BytesIO(obj['Body'].read()))
    df['date'] = pd.to_datetime(df['date'])
    return df

def save_to_s3(df, bucket, key):
    s3_resource = boto3.resource('s3')
    csv_buffer = StringIO()
    df.to_csv(csv_buffer,index=False)
    s3_resource.Object(bucket, key).put(Body=csv_buffer.getvalue())

def get_select_options(df, track_df):
    b=track_df.groupby(['selection']).agg({'count':'sum'}).sort_values(['count'], ascending=False).reset_index(drop=False)
    a=pd.merge(df, b, left_on='title',right_on='selection', how = 'left').sort_values(['count'], ascending=False)
    # a=a['title'].unique()
    # a=np.insert(a,0,'')
    a['date'] = pd.to_datetime(a['date'])
    a=a.groupby('title').agg({'date':'max','count':'first'}).reset_index().sort_values('count',ascending=False)
    a['date'] = a['date'].dt.floor('Min').astype('str').str[:16].str[5:]
    a=a['title'] + ' | ' + a['date']
    a=a.to_list()
    a=np.insert(a,0,'')
    return a

def table_output(df):
    st.write(df.groupby(['Winner']).agg({'Date':'max','Price': ['last','mean','max','min',max_minus_min,'count']}).sort_values([('Price', 'mean')], ascending=True))

def line_chart(df, option):
    g=px.line(df,
    x='Date',
    y='Price',
    color='Winner',
    color_discrete_sequence=['#FF1493','#120052','#652EC7','#00C2BA','#82E0BF','#55E0FF'],
    title='Betting Odds Over Time')
    g.update_traces(mode='lines',
                    opacity=.75,
                    line = dict(width=4))
    g.update_yaxes(title='Implied Probability',
                   showgrid=True,
                   gridwidth=1,
                   gridcolor='#D4D4D4'
                  )
    g.update_layout(plot_bgcolor='white')
    g.update_xaxes(title='Date',
                  showgrid=False,
                  gridwidth=1,
                  gridcolor='#D4D4D4')
    st.plotly_chart(g)

def line_chart_probability(df,option):
    g=px.line(df,
    x='Date',
    y='Implied_Probability',
    color='Winner',
    color_discrete_sequence=['#FF1493','#120052','#652EC7','#00C2BA','#82E0BF','#55E0FF'],
    title='Implied Probability Over Time')
    g.update_traces(mode='lines',
                    opacity=.75,
                    line = dict(width=4))
    g.update_yaxes(range=[0, 1],
                   title='Implied Probability',
                   showgrid=True,
                   gridwidth=1,
                   gridcolor='#D4D4D4',
                   tickformat = ',.0%'
                  )
    g.update_layout(plot_bgcolor='white')
    g.update_xaxes(title='Date',
                  showgrid=False,
                  gridwidth=1,
                  gridcolor='#D4D4D4')
    st.plotly_chart(g)


# def line_chart_favorites(df, option):
    #today = pd.Timestamp.today()
    #last_7 = today - timedelta(days=7)
    #g=px.line(df.loc[(df.Price < 5000) & (df.Date > last_7)], x='Date', y='Price', color='Winner', title='Betting Odds Over Time -- Recent & Favorites')
    # g=px.line(df.loc[(df.Price < 5000)], x='Date', y='Price', color='Winner', title='Betting Odds Over Time -- Recent & Favorites')
    # g.update_traces(mode='lines+markers')
    # st.plotly_chart(g)

# def scatter_plot(df, option):
#     s=df.groupby(['Winner']).agg({'Date':'max','Price': ['last','mean','max','min','count']}).sort_values([('Price', 'mean')], ascending=True)
#     s=s.reset_index(drop = False)
#     s.columns=['Winner','Date','Last Price','Average','Max','Min','Count']
#     g=px.scatter(s, x='Average',y='Last Price',color='Winner', title='Average Bet Price vs. Most Recent')
#     st.plotly_chart(g)

# def bovada_request(url, df):
#     req = requests.get(url)
#     for i in range(json_normalize(req.json()).index.size):
#         for j in range(json_normalize(req.json()[i]['events']).index.size):
#             for k in range(json_normalize(req.json()[i]['events'][j]['displayGroups']).index.size):
#                 for l in range(json_normalize(req.json()[i]['events'][j]['displayGroups'][k]['markets']).index.size):
#                     d=json_normalize(req.json()[i]['events'][j]['displayGroups'][k]['markets'][l]['outcomes'])
#                     a=json_normalize(req.json()[i]['path'])
#                     d['group'] = a['description'].loc[0]
#                     d['title'] = req.json()[i]['events'][j]['description'] + ' - ' + req.json()[i]['events'][j]['displayGroups'][k]['markets'][l]['description']
#                     d['url'] = url
#                     d['date'] = datetime.datetime.now()
#                     try:
#                         df = pd.concat([df, d], sort=False)
#                     except:
#                         df=d
#     df['price.american']=df['price.american'].replace('EVEN',0)
#     df['price.american']=df['price.american'].astype('int')
#     df['date'] = pd.to_datetime(df['date'])
#
#     l = ['Powerball','Dow','California State Lottery','Nasdaq','Ibovespa','S&P 500','Russell 2000','Mega Millions','New York Lotto']
#
#     for i in l:
#         df = df.loc[-df.title.str.contains(i)]
#     return df

def bovada_data():
    url_list = [
    'https://www.bovada.lv/services/sports/event/coupon/events/A/description/soccer?marketFilterId=rank&preMatchOnly=true&lang=en',
    'https://www.bovada.lv/services/sports/event/coupon/events/A/description/entertainment?marketFilterId=def&preMatchOnly=true&lang=en',
    'https://www.bovada.lv/services/sports/event/coupon/events/A/description/basketball?marketFilterId=rank&preMatchOnly=true&lang=en',
    'https://www.bovada.lv/services/sports/event/coupon/events/A/description/politics?marketFilterId=rank&preMatchOnly=true&lang=en',
    'https://www.bovada.lv/services/sports/event/coupon/events/A/description/football?marketFilterId=rank&preMatchOnly=true&lang=en',
    'https://www.bovada.lv/services/sports/event/coupon/events/A/description/baseball?marketFilterId=rank&preMatchOnly=true&lang=en',
    'https://www.bovada.lv/services/sports/event/coupon/events/A/description/boxing?marketFilterId=def&preMatchOnly=true&lang=en',
    'https://www.bovada.lv/services/sports/event/coupon/events/A/description/basketball/college-basketball?marketFilterId=rank&preMatchOnly=true&lang=en'
    ]

    df = pd.DataFrame()
    for url in url_list:
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
    l = ['Powerball','Dow','California State Lottery','Nasdaq','Ibovespa','S&P 500','Russell 2000','Mega Millions','New York Lotto']
    for i in l:
        df = df.loc[-df.title.str.contains(i)]
    return df

# @st.cache
# def update_bovada(df, url):
#     for i in range(len(url)):
#         df = bovada_request(url[i],df)
#
#     bucket = 'bovada-scrape'
#     df_file = 'bovada_requests.csv'
#     save_to_s3(df, bucket, df_file)
#     return df


def ga(event_category, event_action, event_label):
    st.write('<img src="https://www.google-analytics.com/collect?v=1&tid=UA-18433914-1&cid=555&aip=1&t=event&ec='+event_category+'&ea='+event_action+'&el='+event_label+'">',unsafe_allow_html=True)


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

    df = get_s3_data(bucket,df_file)

    df = df.sort_values(['title','description','date'],ascending=True).reset_index(drop=True)

    last_run = (datetime.datetime.now() - df.date.max()).total_seconds()/60/60
    st.write(str(datetime.datetime.now()))
    st.write(str(df.date.max()))
    st.write('Data updated ' + str(round(last_run,1)) + ' hours ago')
    st.write(sys.prefix)

    track_df = get_s3_data(bucket,track_file)
    ga('bovada','get_data',str(df.index.size))

    a=get_select_options(df, track_df)
    option=st.selectbox('Select a bet -', a)
    option = option[:-14]
    print(option)

    if len(option) > 0:
        try:
            track_df = track_df.append({'date' : datetime.datetime.now()
                             ,'selection' :  option,
                             'count' : 1} , ignore_index=True)
            save_to_s3(track_df, bucket, track_file)

            st.markdown('# '+option)
            filtered_df = df.loc[df.title == option]
            filtered_df = filtered_df[['date','url','title','description','price.american','Implied_Probability']].reset_index(drop=True)
            filtered_df.columns = ['Date','URL','Title','Winner','Price','Implied_Probability']
            filtered_df['Date'] = pd.to_datetime(filtered_df['Date'])

            line_chart(filtered_df,option)
            line_chart_probability(filtered_df,option)
            table_output(filtered_df)
            ga('bovada','view_option',option)

        except:
            st.markdown('Select an option above')

if __name__ == "__main__":
    #execute
    main()
