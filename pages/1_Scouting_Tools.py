import streamlit as st
import pandas as pd
from selectolax.parser import HTMLParser
from data_viz.data_viz import pizza_chart
from sqlalchemy import create_engine
import httpx, re
import scrapers.fbrefscraper as fbrefscraper

# TODO player comparison tool
# TODO league leaders
# TODO team stats

st.title("Scouting Tools")

load = st.button('Update Player List')
if load:
    fbrefscraper.main()

st.header("Pizza Charts")

def load_data(table):
    engine = create_engine('sqlite:///fpl-draft-db.db')
    conn = engine.connect()
    return pd.read_sql_table(table, conn)

def scout_report(player_name):
    frame = load_data("fbref_info")
    frame = frame[frame['name'] == player_name]
    url = f"https://fbref.com/en/players/{frame.iloc[0,2]}/scout/365_m1/{frame.iloc[0,3]}-Scouting-Report"

    client = httpx.Client()
    resp = client.get(url)
    html = HTMLParser(resp.text)

    compared_to = html.css_first("a.sr_preset").text()
    competition = html.css_first("div.section_heading_text").text().strip()
    position = html.text(strip=True).split("Position:")[1].split("▪")[0]
    gk = False
    if re.findall('GK', position):
        gk = True

    stats = pd.read_html(url)[0]
    stats.columns = stats.columns.droplevel(0)
    stats = stats.dropna()

    fig = pizza_chart(player_name, stats, compared_to, competition, gk=gk)
    return fig

def players_list():
    frame = load_data("fbref_info")
    return frame.name.to_list()

list_of_players = players_list()

player_dropdown = st.selectbox("Pick a player", list_of_players)
if player_dropdown:
    st.pyplot(fig = scout_report(player_dropdown))

st.header("League Leaders")

