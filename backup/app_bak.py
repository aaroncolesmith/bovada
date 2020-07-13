import pandas as pd
import datetime
from selenium import webdriver
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
import csv
import numpy as np
import plotly.express as px
import streamlit as st
import os
import boto3
import io
from io import StringIO

chrome_options = webdriver.ChromeOptions()
chrome_options.binary_location = os.environ.get("GOOGLE_CHROME_BIN")
chrome_options.add_argument("--headless")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--no-sandbox")
#set this for Heroku

browser = webdriver.Chrome(executable_path=os.environ.get("CHROMEDRIVER_PATH"), chrome_options=chrome_options)
#set this for Local
#browser = webdriver.Chrome('./chromedriver',chrome_options=chrome_options)

pd.set_option('display.max_colwidth', 1000)


def max_minus_min(x): return max(x) - min(x)
def last_minus_avg(x): return last(x) - mean(x)

def bovada_scrape(url,in_df):
    browser.get(url)
    browser.implicitly_wait(5)

    title = browser.find_elements_by_class_name('market-header')
    heading = browser.find_elements_by_class_name('game-heading')
    sub_title = browser.find_elements_by_class_name('market-name')
    outcomes = browser.find_elements_by_class_name('outcomes')
    bet_price = browser.find_elements_by_class_name('bet-price')
    now = datetime.datetime.now()

    if len(outcomes) > 0:
        status = 'Scrape successful'
    else:
        status = 'No data on the web page, may need to update url!'

    #in_df = pd.read_csv(file)

    o = []
    b = []
    t = []

    for i in range(len(outcomes)):
        o.append(outcomes[i].text)
        b.append(bet_price[i].text)
        t.append(title[0].text)
    df = pd.DataFrame({'outcomes':o,'bet_price':b, 'title':t})
    df['date'] = now
    df['bet_price']=df['bet_price'].replace('EVEN', '0')
    df.bet_price = df.bet_price.astype(float)

    df = in_df.append(df,sort=False)
    file = './all.csv'
    df.to_csv(file, index = None)
    df = pd.read_csv(file)

    browser.close()

    return df

def time_since_last_run(df, option):
    try:
        df = df.loc[df.title == option]
        now=pd.to_datetime(datetime.datetime.now())
        max_date = pd.to_datetime(df.date.max())
        time_since = (now-max_date).total_seconds()
    except Exception:
            time_since = 86401
    return time_since

def bovada_graph(df, option):
    df = df.loc[df.title == option]
    bet_title = 'Title Holder'
    st.write(df.groupby(['outcomes']).agg({'date':'max','bet_price': ['last','mean','max','min',max_minus_min,'count']}).sort_values([('bet_price', 'mean')], ascending=True))

    j=px.line(df, x="date", y="bet_price", color='outcomes', width=800, height=600, title=option +' Betting Odds Over Time')
    j.update_xaxes(title='')
    j.update_yaxes(title='Outcomes')
    j.update_traces(mode='lines+markers')
    j.update_layout(
        legend=dict(
            #x=-.5,
            #y=1,
            traceorder="normal",
            font=dict(
                family="sans-serif",
                size=10,
                color="black"
            ),
            #bgcolor="LightSteelBlue",
            #bordercolor="Black",
            #borderwidth=2,
            orientation="h"
        )
    )
    st.plotly_chart(j)

    k=px.line(df.loc[df.bet_price < 5000], x="date", y="bet_price", color='outcomes', width=800, height=600, title=option + ' Betting Odds Over Time (Less than +5000)')
    k.update_xaxes(title='')
    k.update_yaxes(title='Outcomes')
    k.update_traces(mode='lines+markers')
    k.update_layout(
        legend=dict(
            #x=-.5,
            #y=1,
            traceorder="normal",
            font=dict(
                family="sans-serif",
                size=10,
                color="black"
            ),
            #bgcolor="LightSteelBlue",
            #bordercolor="Black",
            #borderwidth=2,
            orientation="h"
        )
    )
    #'Max bet price is' + str(df.bet_price.max())
    if df.bet_price.max() >= 7500:
        st.plotly_chart(k)

def save_to_s3(df):
    bucket = 'bovada-scrape'
    csv_buffer = StringIO()
    df = df[['outcomes','bet_price','title','date']]
    df.to_csv(csv_buffer,index=False)
    s3_resource = boto3.resource('s3')
    now = datetime.datetime.now()
    filename = 'df'+str(now)+'.csv'
    s3_resource.Object(bucket, filename).put(Body=csv_buffer.getvalue())
    s3_resource.Object(bucket, 'df.csv').put(Body=csv_buffer.getvalue())

def get_df_s3(bucket, key):
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(io.BytesIO(obj['Body'].read()))
    return df

data = [[' Select an option',''],
['SUPER BOWL 54 (2020) - Odds To Win', 'https://www.bovada.lv/sports/football/nfl-futures/super-bowl-54-2020-odds-to-win-202002021130'],
['Odds To Win Best Actor', 'https://www.bovada.lv/sports/entertainment/academy-awards/92nd-academy-awards/odds-to-win-best-actor-202002092352'],
['Odds To Win Best Picture', 'https://www.bovada.lv/sports/entertainment/academy-awards/92nd-academy-awards/odds-to-win-best-picture-202002092350'],
['FIFA World Cup 2022','https://www.bovada.lv/sports/soccer/fifa-world-cup/fifa-world-cup-2022/fifa-world-cup-2022-202008131000'],
['SUPER BOWL 55 (2021) - Odds To Win','https://www.bovada.lv/sports/football/nfl-futures/super-bowl-55-2021-odds-to-win-202101012015'],
['US Presidential Election 2020 - Odds To Win','https://www.bovada.lv/sports/politics/us-politics/us-presidential-election-2020/us-presidential-election-2020-odds-to-win-202003012259'],
['2020 NBA Championship - Odds To Win','https://www.bovada.lv/sports/basketball/nba/2020-nba-championship-odds-to-win-202006011759'],
['2020 World Series','https://www.bovada.lv/sports/baseball/mlb/2020-world-series-202002291300'],
['Odds To Win The 2020 XFL Championship Game','https://www.bovada.lv/sports/football/xfl/odds-to-win-the-2020-xfl-championship-game-202002081401'],
['Odds To Win 2020 NCAA Men\'s Basketball Tournament','https://www.bovada.lv/sports/basketball/college-basketball/odds-to-win-2020-ncaa-men-s-basketball-tournament-202003011730'],
['The Masters 2020','https://www.bovada.lv/sports/golf/golf-futures/the-masters-2020-202004090700']]

bet_url = pd.DataFrame(data, columns = ['bet_option', 'url']).sort_values(['bet_option'], ascending=True)

#file = './all.csv'
bucket = 'bovada-scrape'
key = 'df.csv'
df = get_df_s3(bucket, key)

st.sidebar.markdown('Welcome to Bovada Scrape!!! Select an option below and see a visualization to the right!')
option=st.sidebar.selectbox('Select a bet -',bet_url.bet_option.unique().tolist())

url=bet_url['url'].loc[bet_url.bet_option == option].to_string(index=False)

t=time_since_last_run(df, option)
t=round(t,2)

t_threshold = 300

if t > t_threshold:
    st.sidebar.markdown('Last run was ' + str(round(t/60/60,2)) + ' hours ago. Scraping Bovada at' + url + '. This may take about 30 seconds.')
    df = bovada_scrape(url,df)
elif option == ' Select an option':
    st.sidebar.markdown('')
elif t <= t_threshold:
    st.sidebar.markdown('Last run was ' + str(round(t/60,2)) + ' minutes ago. Using existing file')
    #df = pd.read_csv(file)
else:
    st.sidebar.markdown('New file. Scraping Bovada at' + url + '. This may take about 30 seconds.')
    df = bovada_scrape(url,df)

if option != ' Select an option':
    bovada_graph(df, option)
    save_to_s3(df)
