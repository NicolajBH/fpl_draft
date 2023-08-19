import httpx
from sqlalchemy import create_engine, text
from datetime import datetime, timezone
import pandas as pd
import numpy as np
import time

def gw_to_scrape(conn):
    '''
    Gets the latest deadline from sql database
    If it fails (e.g database doesn't exist) it makes a request to the fpl api to get the latest deadline
    '''
    try:
        query = "SELECT MAX(id) FROM deadlines WHERE finished=1"
        start_gw = pd.read_sql(text(query),conn).values[0][0]
        now = datetime.now(timezone.utc)
        if start_gw == 38:
            end_gw = 38
        else:
            query2 = "SELECT deadline_time FROM deadlines WHERE id = (SELECT MIN(id) FROM deadlines WHERE finished = 0)"
            next_deadline = pd.read_sql(text(query2),conn).values[0][0]
            next_deadline = datetime.strptime(next_deadline, "%Y-%m-%dT%H:%M:%S%z")
            if now > next_deadline:
                end_gw = start_gw+1
            else:
                end_gw = start_gw
    except:
        url = "https://draft.premierleague.com/api/game"
        data = get_data(url)
        start_gw = 1
        end_gw = data['current_event']
    return start_gw, end_gw

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

def update_info(engine):
    '''
    Updates info in deadline, fantasy player info and draft player info
    '''
    print("Updating records...")
    # deadline info
    deadlines = get_data("https://draft.premierleague.com/api/bootstrap-static")
    deadlines = pd.DataFrame(deadlines['events']['data'])
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

def update_player_stats(engine,start_gw,end_gw):
    '''
    Appends new gameweek data from the fpl api between the gameweeks specified (inclusive)
    '''
    for gw in range(start_gw, end_gw+1):
        print(f"Updating player stats for gameweek {gw}")
        url = f"https://fantasy.premierleague.com/api/event/{gw}/live"
        data = get_data(url)
        player_stats_new_rows = pd.json_normalize(data,record_path=['elements']).drop(columns=['explain'])
        player_stats_new_rows['gw'] = gw
        player_stats_new_rows['TimeStamp'] = datetime.now(timezone.utc)
        player_stats_new_rows.to_sql('player_stats',engine,if_exists='append', index=False)
        print(str(len(player_stats_new_rows)) + ' rows added to player_stats')

def update_player_picks(engine,start_gw,end_gw,players):
    '''
    Requests data for player picks for each team in players dict between start_gw and end_gw inclusive
    Adds additional columns for subs
    '''
    player_picks_df = pd.DataFrame()
    for gw in range(start_gw, end_gw+1):
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
            
def last_updated():
    engine = create_engine('sqlite:///fpl-draft-db.db')
    conn = engine.connect()
    query = "SELECT MAX(TimeStamp) FROM player_picks"
    last_update = pd.read_sql(text(query),conn).values[0][0]
    return last_update

def main():
    players = {545927:'Nicolaj',546201:'Jesus',525936:'Kris',527284:'Mattia',524333:'Ollie'}
    engine = create_engine('sqlite:///fpl-draft-db.db')
    conn = engine.connect()

    start_gw, end_gw = gw_to_scrape(conn)
    print(f"Scraping data from gameweek {start_gw} to {end_gw}...")

    update_info(engine)
    update_player_stats(engine, start_gw, end_gw)
    update_player_picks(engine, start_gw, end_gw, players)

    print("Cleaning SQL database...")
    sql_remove_duplicates(engine, "element", "player_picks")
    sql_remove_duplicates(engine, "id", "player_stats")

    print("Complete.")

if __name__ == "__main__":
    main()