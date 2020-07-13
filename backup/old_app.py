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

options = Options()
options.headless = True

def max_minus_min(x): return max(x) - min(x)
def last_minus_avg(x): return last(x) - mean(x)

def bovada_scrape(url, file):
    browser = webdriver.Chrome('./chromedriver',options=options)
    browser.get(url)
    browser.implicitly_wait(5)

    title = browser.find_elements_by_class_name('market-header')
    heading = browser.find_elements_by_class_name('game-heading')
    sub_title = browser.find_elements_by_class_name('market-name')
    outcomes = browser.find_elements_by_class_name('outcomes')
    bet_price = browser.find_elements_by_class_name('bet-price')
    now = datetime.datetime.now()

    try:
        in_df = pd.read_csv(file)
    except Exception:
        try:
            del in_df
        except Exception:
            pass

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

    try:
        df = in_df.append(df)
    except Exception:
        pass

    df.to_csv(file, index = None)
    df = pd.read_csv(file)

    browser.close()

    return df

def time_since_last_run(file):
    try:
        df = pd.read_csv(file)
        now=pd.to_datetime(datetime.datetime.now())
        max_date = pd.to_datetime(df.date.max())
        time_since = (now-max_date).total_seconds()
    except Exception:
            time_since = 86401
    return time_since

#option = st.selectbox('Which bet would you like to view?',('Oscars - Best Picture', 'Oscars - Best Actor', 'President'))
#url='https://www.bovada.lv/sports/entertainment/academy-awards/92nd-academy-awards/odds-to-win-best-actor-202002091152'
#file='./oscars_best_actor.csv'

url='https://www.bovada.lv/sports/politics/us-politics/2020-us-presidential-election/us-presidential-election-2020-odds-to-win-202003012259'
file='./next_pres.csv'

t=time_since_last_run(file)
t=round(t,2)

if t > 86400:
    'Last run was ' + str(t) + ' seconds ago. Scraping Bovada'
    df = bovada_scrape(url,file)
else:
    'Last run was ' + str(t) + ' seconds ago. Using existing file'
    df = pd.read_csv(file)

bet_title = df['title'].loc[0]

#bet_title

st.write(df.groupby(['outcomes']).agg({'bet_price': ['last','mean','max','min',max_minus_min,'count']}).sort_values([('bet_price', 'mean')], ascending=True))

j=px.line(df, x="date", y="bet_price", color='outcomes', width=800, height=600, title=bet_title +' Betting Odds Over Time')
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

k=px.line(df.loc[df.bet_price < 5000], x="date", y="bet_price", color='outcomes', width=800, height=600, title=bet_title + ' Betting Odds Over Time (Less than +5000)')
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
