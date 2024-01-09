import streamlit as st
import scrapers.data_wrangling as data_wrangling
import data_viz.data_viz as data_viz
import scrapers.scraper as scraper
from sqlalchemy import create_engine

st.set_page_config(
    page_title="Clueless",
    page_icon="⚽️",
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
# st.image("data_viz/figures/weekwise_table.png")

st.header("Monthly Standings")
months_dropdown = st.selectbox("Pick a month", data_wrangling.list_of_months())
stat_dropdown = st.multiselect("Pick stats",data_wrangling.list_of_stats(), key="1")
st.dataframe(data_wrangling.draft_standings(by_month=True, month=months_dropdown, stats_to_display=stat_dropdown))

st.header("Gameweek Standings")
min_gw, max_gw = data_wrangling.min_max_gw()
if max_gw != 1:
    gws = st.slider('Select gameweeks',int(min_gw),int(max_gw), (int(min_gw),int(max_gw)), key="2")
if max_gw == 1:
    gws = [1,1]
stat_list = data_wrangling.list_of_stats()
stat_list.remove('Team')
stat_dropdown_gw = st.multiselect("Pick stats",stat_list, key="3")
st.dataframe(data_wrangling.draft_standings(by_gw=True, gws=gws, stats_to_display=stat_dropdown_gw))

st.header("Player Stats")
if max_gw != 1:
    gws_player_stats = st.slider('Select gameweeks',int(min_gw),int(max_gw), (int(min_gw),int(max_gw)), key="4")
if max_gw == 1:
    gws_player_stats = [1,1]
to_remove = ['Best Player','Best Player Points','Points Benched','Points Subbed On','Number Of Subs']
stat_list_players = [i for i in stat_list if i not in to_remove]
player_stat_dropdown_gw = st.multiselect("Pick stats",stat_list_players, key="5")
by_team_checkbox = st.checkbox('By Team')
st.dataframe(data_wrangling.player_stat_menu(by_gw=True, by_team=by_team_checkbox, gws=gws_player_stats, stats_to_display=player_stat_dropdown_gw))