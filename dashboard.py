import httpx, time
import streamlit as st
from sqlalchemy import create_engine, text, inspect, update
from datetime import datetime, timezone
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# Import Data from FPL API into SQL db
# Team ID and Player Names for players in the draft league
players = {545927:'Nicolaj',546201:'Jesus',525936:'Kris',527284:'Mattia',524333:'Ollie'}

def gw_to_scrape(conn):
    '''
    Checks fpl-draft-db.db for existing data and returns list of gameweeks to scrape.
    '''
    query = "SELECT id \
            FROM deadlines \
            WHERE (isScraped = 0 AND finished = 1)\
            OR (isScraped = 0 AND finished = 0 AND datetime('now') > datetime(deadline_time))"
    gws = pd.read_sql(text(query),conn).values.tolist()
    gws = [element for innerList in gws for element in innerList]
    return gws

def get_data(url):
    '''
    Makes request to fpl api and returns data in json format
    '''
    headers = {
        "Cookie": "pl_euconsent-v2=CPnHJ0HPnHJ0HFCABAENC3CsAP_AAH_AAAwIF5wAQF5gXnABAXmAAAAA.YAAAAAAAAAAA; pl_euconsent-v2-intent-confirmed={%22tcf%22:[755]%2C%22oob%22:[]}; pl_oob-vendors={}"
    }
    response = httpx.get(url, headers=headers, follow_redirects=True)
    while response.status_code != 200:
        print(response.status_code)
        time.sleep(5)
        response = httpx.get(url, headers=headers)
    return response.json()

def update_info(engine, conn):
    '''
    Updates info in deadline, fantasy player info and draft player info
    '''
    print("Updating records...")
    # deadline info
    insp = inspect(engine)
    check = insp.has_table("deadlines")
    if check == True:
        deadlines = pd.read_sql_table("deadlines", conn)
        new_deadlines = get_data("https://draft.premierleague.com/api/bootstrap-static")
        new_deadlines = pd.DataFrame(new_deadlines['events']['data'])
        deadlines['deadline_time'] = new_deadlines['deadline_time']
        deadlines['trades_time'] = new_deadlines['trades_time']
        deadlines['waivers_time'] = new_deadlines['waivers_time']
        deadlines['finished'] = new_deadlines['finished']
    else:
        deadlines = get_data("https://draft.premierleague.com/api/bootstrap-static")
        deadlines = pd.DataFrame(deadlines['events']['data'])
        deadlines['isScraped'] = False
    
    deadlines['month'] = pd.DatetimeIndex(deadlines['deadline_time']).month_name()
    deadlines.to_sql('deadlines',engine,if_exists='replace', index=False)
    print("Deadline info updated")

    # player info from fantasy premier league
    fpl_player_info = get_data("https://fantasy.premierleague.com/api/bootstrap-static/")
    fpl_player_info = pd.DataFrame(fpl_player_info['elements'])
    fpl_player_info.to_sql('fantasy_player_info',engine,if_exists='replace',index=False)
    print(str(len(fpl_player_info)) + " records updated for fpl player info")

    # player info from draft premier league
    draft_player_info = get_data("https://draft.premierleague.com/api/bootstrap-static")
    draft_player_info = pd.DataFrame(draft_player_info['elements'])
    draft_player_info.to_sql('draft_player_info',engine,if_exists='replace', index=False)
    print(str(len(draft_player_info)) + " records updated for draft player info")

def update_player_stats(engine,gws_to_scrape):
    '''
    Appends new gameweek data from the fpl api between the gameweeks specified (inclusive)
    '''
    for gw in gws_to_scrape:
        print(f"Updating player stats for gameweek {gw}")
        url = f"https://fantasy.premierleague.com/api/event/{gw}/live"
        data = get_data(url)
        player_stats_new_rows = pd.json_normalize(data,record_path=['elements']).drop(columns=['explain'])
        player_stats_new_rows['gw'] = gw
        player_stats_new_rows['TimeStamp'] = datetime.now(timezone.utc)
        player_stats_new_rows.to_sql('player_stats',engine,if_exists='append', index=False)
        print(str(len(player_stats_new_rows)) + ' rows added to player_stats')

def update_player_picks(engine,gws_to_scrape):
    '''
    Requests data for player picks for each team in players dict between start_gw and end_gw inclusive
    Adds additional columns for subs
    '''
    player_picks_df = pd.DataFrame()
    for gw in gws_to_scrape:
        print(f"Retrieving player picks for gameweek {gw}")
        for uid, name in players.items():
            url = f"https://draft.premierleague.com/api/entry/{uid}/event/{gw}"
            data = get_data(url)
            picks_new_rows = pd.DataFrame(data['picks'])
            sub_in = [d['element_in'] for d in data['subs'] if 'element_in' in d]
            sub_out = [d['element_out'] for d in data['subs'] if 'element_out' in d]
            picks_new_rows['gw'] = gw
            picks_new_rows['team_id'] = uid
            picks_new_rows['team_name'] = name
            picks_new_rows['played'] = np.where(picks_new_rows.position <= 11, True, False)
            picks_new_rows['sub_in'] = np.where(picks_new_rows.element.isin(sub_in), True, False)
            picks_new_rows['sub_out'] = np.where(picks_new_rows.element.isin(sub_out), True, False)
            picks_new_rows['TimeStamp'] = datetime.now(timezone.utc)
            picks_new_rows.drop(columns=['is_captain','is_vice_captain','multiplier'], inplace=True)
            player_picks_df = pd.concat([player_picks_df,picks_new_rows])
    player_picks_df.to_sql('player_picks',engine,if_exists='append', index=False)
    print(str(len(player_picks_df)) + ' rows added to player_picks')

def sql_remove_duplicates(engine,id_column_name,table):
    '''
    Used to remove outdated records
    '''
    with engine.connect() as conn:
        with conn.begin():
            conn.execute(text(
                f"delete from {table} where TimeStamp not in (\
                    select max(Timestamp)\
                    from {table}\
                    group by {id_column_name}, gw)"))

def fpl_updater():
    engine = create_engine('sqlite:///fpl-draft-db.db')
    conn = engine.connect()

    update_info(engine, conn)

    gws_to_scrape = gw_to_scrape(conn)
    print(f'Scraping data from {gws_to_scrape}')

    update_player_stats(engine, gws_to_scrape)
    update_player_picks(engine, gws_to_scrape)

    print("Cleaning SQL database...")
    sql_remove_duplicates(engine, "element", "player_picks")
    sql_remove_duplicates(engine, "id", "player_stats")

    with engine.connect() as conn:
        with conn.begin():
            conn.execute(text(
                "UPDATE deadlines \
                SET isScraped = 1 \
                WHERE finished = 1 AND isScraped=0"))

# Wrangle Data
            
def load_data(table):
    engine = create_engine('sqlite:///fpl-draft-db.db')
    conn = engine.connect()
    return pd.read_sql_table(table, conn)

draft_player_info = load_data('draft_player_info')
fantasy_player_info = load_data('fantasy_player_info')
player_info = draft_player_info.merge(fantasy_player_info,
                                on=['first_name','second_name','web_name','element_type','team']).\
                                    rename(columns={'id_x':'draft_id','id_y':'fpl_id'})
player_stats = load_data('player_stats')
convert_dict = {col:'float' for col in player_stats.select_dtypes('object').columns.to_list()}
player_stats = player_stats.astype(convert_dict)

player_picks = load_data('player_picks')
deadlines = load_data('deadlines')

frame = player_picks.merge(player_info, left_on='element', right_on='draft_id')
frame = frame.merge(player_stats, left_on=['fpl_id','gw'], right_on=['id','gw'])
frame = frame.merge(deadlines[['deadline_time','month','id']],
                    left_on='gw',right_on='id')\
                        .drop(columns=['id_y','TimeStamp_y'])

def top_scoring_player(data):
    best_players = data.groupby(by=['team_id','element']).sum(numeric_only=True).reset_index()\
        .sort_values(by=['stats.total_points','draft_rank'], ascending=[False,True]).drop_duplicates(subset='team_id')
    best_players = best_players.merge(player_info[['draft_id','web_name']], left_on='element',right_on='draft_id')
    return best_players

def draft_standings(by_gw=False, by_month=False, gws=[], month="", stats_to_display=[]):
    if by_gw:
        data = frame[frame.gw.between(gws[0],gws[1])]
    if by_month:
        data = frame[frame.month == month]
    else:
        data = frame
    
    standings = data[data.played == True].groupby(by=['team_id']).sum(numeric_only=True).reset_index()
    top_scorer = top_scoring_player(data)
    standings = standings.merge(top_scorer, on='team_id')
    standings = standings.sort_values(by='stats.total_points_x', ascending=False).reset_index()

    benched = data[data.played == False].groupby(by=['team_id']).sum(numeric_only=True).reset_index()
    benched = benched[['team_id','stats.total_points']]
    benched.rename(columns = {'stats.total_points':'stats.points_benched'}, inplace=True)
    standings = standings.merge(benched, on='team_id', how='left')

    points_subbed_on = data[data.sub_in == True].groupby(by=['team_id']).sum(numeric_only=True).reset_index()
    points_subbed_on = points_subbed_on[['team_id','stats.total_points']]
    points_subbed_on.rename(columns = {'stats.total_points':'stats.points_subbed_on'}, inplace=True)
    standings = standings.merge(points_subbed_on, on='team_id', how='left')

    players_subbed_on = data[data.sub_in == True].groupby(by=['team_id'])['team_id'].value_counts().rename_axis('team_id').reset_index(name='stats.number_of_subs')
    standings = standings.merge(players_subbed_on, on='team_id', how='left')

    cols = [
        'team_id','stats.total_points_x','web_name','stats.total_points_y','stats.goals_scored_x','stats.assists_x','stats.clean_sheets_x',
        'stats.goals_conceded_x','stats.own_goals_x','stats.penalties_saved_x','stats.penalties_missed_x','stats.yellow_cards_x',
        'stats.red_cards_x','stats.saves_x','stats.bonus_x','stats.bps_x','stats.expected_goals_x','stats.expected_assists_x',
        'stats.expected_goal_involvements_x','stats.expected_goals_conceded_x','stats.in_dreamteam_x', 'stats.points_benched', 
        'stats.points_subbed_on', 'stats.number_of_subs'
    ]
    col_rename = [i.strip("_x").replace("stats.", "").replace("_"," ").title() for i in cols]
    standings = standings[cols]
    standings.columns = col_rename
    players = {545927:'Nicolaj',546201:'Jesus',525936:'Kris',527284:'Mattia',524333:'Ollie'}
    standings.replace({'Team Id': players}, inplace=True)
    standings.rename(columns = {
        "Total Points":"Points",
        "Web Name":"Best Player",
        "Total Points Y":"Best Player Points",
        "Team Id":"Team"
    }, inplace=True)
    if len(stats_to_display) == 0:
        stats_to_display = list(standings.columns)
    else:
        stats_to_display.insert(0, "Team")
    standings.index += 1
    return standings[stats_to_display]

def player_stat_menu(by_gw=False, by_team=False, gws=[], stats_to_display=[]):
    if by_gw:
        df = frame[frame.gw.between(gws[0],gws[1])]
    cols = [
            'team_id','web_name','stats.total_points','stats.goals_scored','stats.assists','stats.clean_sheets',
            'stats.goals_conceded','stats.own_goals','stats.penalties_saved','stats.penalties_missed','stats.yellow_cards',
            'stats.red_cards','stats.saves','stats.bonus','stats.bps','stats.expected_goals','stats.expected_assists',
            'stats.expected_goal_involvements','stats.expected_goals_conceded','stats.in_dreamteam',
        ]

    if by_team:
        df = frame.groupby(by=['team_id','web_name']).sum(numeric_only=True).reset_index()
    else:
        df = frame.groupby(by=['web_name']).sum(numeric_only=True).reset_index()
        cols.remove('team_id')

    col_rename = [i.replace("stats.", "").replace("_"," ").title() for i in cols]
    df = df[cols]
    df.columns = col_rename

    if by_team:
        players = {545927:'Nicolaj',546201:'Jesus',525936:'Kris',527284:'Mattia',524333:'Ollie'}
        df.replace({'Team Id': players}, inplace=True)
        df.rename(columns = {
            "Total Points":"Points",
            "Team Id":"Team"
        }, inplace=True)
    else:
        df.rename(columns = {
            "Total Points":"Points",
        }, inplace=True)
    if (len(stats_to_display) == 0) and (not by_team):
        stats_to_display = list(df.columns)
        column_sort = 1
    elif (len(stats_to_display) == 0) and (by_team):
        stats_to_display = list(df.columns)
        column_sort = 2
    elif by_team:
        stats_to_display.insert(0, "Team")
        stats_to_display.insert(1, "Web Name")
        column_sort = 2
    else:
        stats_to_display.insert(0,'Web Name')
        column_sort = 1
    df = df[stats_to_display].sort_values(by=df[stats_to_display].columns[column_sort], ascending=False).reset_index(drop=True)
    df.index += 1
    return df

def list_of_stats():
    df = draft_standings()
    return list(df.columns)

# Data visualisation

def overall_table_figure():
    data = draft_standings()
    data = data.sort_values(by='Points')

    fig = plt.figure(figsize=(6,2), dpi=300)
    ax = plt.subplot()

    ncols = data.shape[1]
    nrows = data.shape[0]

    ax.set_xlim(0, ncols + 1)
    ax.set_ylim(0, nrows)

    positions = [0.25, 4.25, 8.25, 12.25, 16.25, 20.25]
    columns = ['Team', 'Points', 'Goals Scored', 'Assists', 'Clean Sheets','Best Player']

    # Add table's main text
    for i in range(nrows):
        for j, column in enumerate(columns):
            if j == 0:
                ha = 'left'
            else:
                ha = 'center'
            if column == 'Points':
                text_label = f'{data[column].iloc[i]:,.0f}'
                weight = 'bold'
            else:
                text_label = f'{data[column].iloc[i]}'
                weight = 'normal'
            ax.annotate(
                xy=(positions[j], i + .5),
                text=text_label,
                ha=ha,
                va='center',
                weight=weight,
                color='white'
            )

    # Add column names
    column_names = ['Team', 'Points', 'Goals\nScored', 'Assists', 'Clean\nSheets','Best\nPlayer']
    for index, c in enumerate(column_names):
            if index == 0:
                ha = 'left'
            else:
                ha = 'center'
            ax.annotate(
                xy=(positions[index], nrows),
                text=column_names[index],
                ha=ha,
                va='bottom',
                weight='bold',
                color='white'
            )

    # Add dividing lines
    ax.plot([ax.get_xlim()[0], ax.get_xlim()[1]], [nrows, nrows], lw=1.5, color='white', marker='', zorder=4)
    ax.plot([ax.get_xlim()[0], ax.get_xlim()[1]], [0, 0], lw=1.5, color='white', marker='', zorder=4)
    for x in range(1, nrows):
        ax.plot([ax.get_xlim()[0], ax.get_xlim()[1]], [x, x], lw=1.15, color='gray', ls=':', zorder=3 , marker='')

    ax.set_axis_off()
    plt.savefig(
        'figures/overall_table.png',
        dpi=300,
        transparent=True,
        bbox_inches='tight'
    )

# Display Data on Streamlit

st.set_page_config(
    page_title="Clueless",
    page_icon="⚽️",
)

st.title("Clueless Dashboard")

st.sidebar.success("2023-24 Draft", icon="⚽️")

load = st.button('Update')
if load:
   fpl_updater()
   overall_table_figure()

def last_updated():
    engine = create_engine('sqlite:///fpl-draft-db.db')
    conn = engine.connect()
    query = "SELECT MAX(TimeStamp) FROM player_picks"
    last_update = pd.read_sql(text(query),conn).values[0][0]
    return last_update

last_update = last_updated()
st.text("Last Updated: "+last_update+" UTC")

st.header("Overall Standings")
st.image("figures/overall_table.png")

stat_list = draft_standings()
stat_list = list(stat_list)
stat_list.remove('Team')

months = list(frame.month.unique())
months.reverse()
min_gw, max_gw = frame.gw.min(), frame.gw.max()

st.header("Monthly Standings")
months_dropdown = st.selectbox("Pick a month", months)
stat_dropdown = st.multiselect("Pick stats",stat_list, key="1")
st.dataframe(draft_standings(by_month=True, month=months_dropdown, stats_to_display=stat_dropdown))

st.header("Gameweek Standings")
if max_gw != 1:
    gws = st.slider('Select gameweeks',int(min_gw),int(max_gw), (int(min_gw),int(max_gw)), key="2")
if max_gw == 1:
    gws = [1,1]
stat_dropdown_gw = st.multiselect("Pick stats",stat_list, key="3")
st.dataframe(draft_standings(by_gw=True, gws=gws, stats_to_display=stat_dropdown_gw))

st.header("Player Stats")
if max_gw != 1:
    gws_player_stats = st.slider('Select gameweeks',int(min_gw),int(max_gw), (int(min_gw),int(max_gw)), key="4")
if max_gw == 1:
    gws_player_stats = [1,1]
to_remove = ['Best Player','Best Player Points','Points Benched','Points Subbed On','Number Of Subs']
stat_list_players = [i for i in stat_list if i not in to_remove]
player_stat_dropdown_gw = st.multiselect("Pick stats",stat_list_players, key="5")
by_team_checkbox = st.checkbox('By Team')
st.dataframe(player_stat_menu(by_gw=True, by_team=by_team_checkbox, gws=gws_player_stats, stats_to_display=player_stat_dropdown_gw))