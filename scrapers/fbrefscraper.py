import httpx
from selectolax.parser import HTMLParser
import pandas as pd
from urllib.parse import urljoin
from sqlalchemy import create_engine
import time

def main():
    client = httpx.Client()
    url = "https://fbref.com/en/comps/9/Premier-League-Stats"
    resp = client.get(url)
    html = HTMLParser(resp.text)
    league_table = html.css_first("tbody")

    player_urls = {}
    for team in league_table.css('tr'):
        base_url = "https://fbref.com/"
        url = urljoin(base_url, team.css_first("a").attributes['href'])
        time.sleep(2.5)
        resp = client.get(url)
        html = HTMLParser(resp.text)
        players_table = html.css_first("tbody")
        for player in players_table.css('tr'):
            player_urls[player.css_first('a').text()] = player.css_first('a').attributes['href']
    frame = pd.json_normalize(player_urls).T
    frame.reset_index(inplace=True)
    frame[['1','2','3']] = frame[0].str.rsplit('/', n=2, expand=True)
    frame.rename(columns={
        'index':'name',
        '0':'link',
        '2':'player_id',
        '3':'url_name'
    }, inplace=True)
    frame.drop(columns=['1'], inplace=True)
    
    engine = create_engine('sqlite:///fpl-draft-db.db')
    frame.to_sql('fbref_info',engine,if_exists='replace',index=False)