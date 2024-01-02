import streamlit as st
import scrapers.data_wrangling as data_wrangling
import data_viz.data_viz as data_viz
import scrapers.scraper as scraper
from sqlalchemy import create_engine

st.set_page_config(
    page_title="Clueless",
    page_icon="👋",
)

st.title("Clueless Dashboard")

st.sidebar.success("2022-23 Draft", icon="⚽️")

load = st.button('Update')
if load:
   scraper.main()
   data_viz.main()

last_update = scraper.last_updated()
st.text("Last Updated: "+last_update+" UTC")
st.header("Overall Standings")
st.image("data_viz/figures/overall_table.png")
st.image("data_viz/figures/weekwise_table.png")

st.header("Monthly Standings")
months_dropdown = st.selectbox("Pick a month", data_wrangling.list_of_months())
st.dataframe(data_wrangling.draft_standings(by_month=True, month=months_dropdown))

st.header("Gameweek Standings")
min_gw, max_gw = data_wrangling.min_max_gw()
if max_gw != 1:
    gws = st.slider('Select gameweeks',int(min_gw),int(max_gw), (int(min_gw),int(max_gw)))
if max_gw == 1:
    gws = [1,1]
st.dataframe(data_wrangling.draft_standings(by_gw=True, gws=gws))