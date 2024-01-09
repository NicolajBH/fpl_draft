import pandas as pd
from sqlalchemy import create_engine

def load_data(table):
    engine = create_engine('sqlite:///fpl-draft-db.db')
    conn = engine.connect()
    return pd.read_sql_table(table, conn)

def player_info():
    draft_player_info = load_data('draft_player_info')
    fantasy_player_info = load_data('fantasy_player_info')
    frame = draft_player_info.merge(fantasy_player_info,
                                    on=['first_name','second_name','web_name','element_type','team']).\
                                        rename(columns={'id_x':'draft_id','id_y':'fpl_id'})
    return frame

def player_stats():
    # player stat dtypes
    player_stats = load_data('player_stats')
    convert_dict = {col:'float' for col in player_stats.select_dtypes('object').columns.to_list()}
    return player_stats.astype(convert_dict)

def merge_data():
    player_picks = load_data('player_picks')
    deadlines = load_data('deadlines')
    stats = player_stats()
    info = player_info()

    frame = player_picks.merge(info, left_on='element', right_on='draft_id')
    frame = frame.merge(stats, left_on=['fpl_id','gw'], right_on=['id','gw'])
    frame = frame.merge(deadlines[['deadline_time','month','id']],
                        left_on='gw',right_on='id')\
                            .drop(columns=['id_y','TimeStamp_y'])
    return frame

def top_scoring_player(data):
    best_players = data.groupby(by=['team_id','element']).sum(numeric_only=True).reset_index()\
        .sort_values(by=['stats.total_points','draft_rank'], ascending=[False,True]).drop_duplicates(subset='team_id')
    info = player_info()
    best_players = best_players.merge(info[['draft_id','web_name']], left_on='element',right_on='draft_id')
    return best_players

def draft_standings(by_gw=False, by_month=False, gws=[], month="", stats_to_display=[]):
    data = merge_data()
    if by_gw:
        data = data[data.gw.between(gws[0],gws[1])]
    if by_month:
        data = data[data.month == month]
        
    standings = data[data.played == True].groupby(by=['team_id']).sum(numeric_only=True).reset_index()
    top_scorer = top_scoring_player(data)
    standings = standings.merge(top_scorer, on='team_id')
    standings = standings.sort_values(by='stats.total_points_x', ascending=False).reset_index()

    benched = data[data.played == False].groupby(by=['team_id']).sum(numeric_only=True).reset_index()
    benched = benched[['team_id','stats.total_points']]
    benched.rename(columns = {'stats.total_points':'stats.points_benched'}, inplace=True)
    standings = standings.merge(benched, on='team_id')

    points_subbed_on = data[data.sub_in == True].groupby(by=['team_id']).sum(numeric_only=True).reset_index()
    points_subbed_on = points_subbed_on[['team_id','stats.total_points']]
    points_subbed_on.rename(columns = {'stats.total_points':'stats.points_subbed_on'}, inplace=True)
    standings = standings.merge(points_subbed_on, on='team_id')

    players_subbed_on = data[data.sub_in == True].groupby(by=['team_id'])['team_id'].value_counts().rename_axis('team_id').reset_index(name='stats.number_of_subs')
    standings = standings.merge(players_subbed_on, on='team_id')

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
    data = merge_data()
    if by_gw:
        data = data[data.gw.between(gws[0],gws[1])]
    cols = [
            'team_id','stats.total_points','web_name','stats.goals_scored','stats.assists','stats.clean_sheets',
            'stats.goals_conceded','stats.own_goals','stats.penalties_saved','stats.penalties_missed','stats.yellow_cards',
            'stats.red_cards','stats.saves','stats.bonus','stats.bps','stats.expected_goals','stats.expected_assists',
            'stats.expected_goal_involvements','stats.expected_goals_conceded','stats.in_dreamteam',
        ]

    if by_team:
        df = data.groupby(by=['team_id','web_name']).sum(numeric_only=True).reset_index()
    else:
        df = data.groupby(by=['web_name']).sum(numeric_only=True).reset_index()
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
    df = df.sort_values(by=['Points'], ascending=False).reset_index(drop=True)
    if len(stats_to_display) == 0:
        stats_to_display = list(df.columns)
    df.index += 1
    return df[stats_to_display]

def list_of_stats():
    df = draft_standings()
    return list(df.columns)
    
def list_of_months():
    data = merge_data()
    months = list(data.month.unique())
    months.reverse()
    return months

def min_max_gw():
    data = merge_data()
    min_gw, max_gw = data.gw.min(), data.gw.max()
    return min_gw, max_gw

