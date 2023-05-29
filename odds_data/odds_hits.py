import pandas as pd
import numpy as np
import requests
import statsapi
from datetime import datetime

from static_data.load_static_data import *
import static_data.load_static_data


def fetch_hits_odds_json():
    #url = 'https://sportsbook-us-ny.draftkings.com//sites/US-NY-SB/api/v5/eventgroups/84240/categories/985?format=json'
    url = 'https://sportsbook-us-ny.draftkings.com//sites/US-NY-SB/api/v5/eventgroups/84240/categories/743/subcategories/6719?format=json'

    return requests.get(url).json()

def get_schedule_today():
    date_today = datetime.today().strftime("%Y-%m-%d")
    return statsapi.schedule(start_date = date_today, end_date = date_today)

def find_today_game(schedule, team_name):
    for sch in schedule:
        if sch['away_name'] == team_name or sch['home_name'] == team_name:
            return sch

def find_today_game_id(schedule, team_name):
    game_sch = find_today_game(schedule, team_name)
    if game_sch is None:
        return None
    return game_sch['game_id']

def fetch_df_hits_odd_today():
    js_hits = fetch_hits_odds_json()
    schedule_today = get_schedule_today()

    i_ocat = 0
    for i, cat in enumerate(js_hits['eventGroup']['offerCategories']):
        if cat['name'] == 'Batter Props':
            i_ocat = i
            break
    
    i_ocsubat = 0
    for i, subcat in enumerate(js_hits['eventGroup']['offerCategories'][i_ocat]['offerSubcategoryDescriptors']):
        if subcat['name'] == 'Hits':
            i_ocsubat = i
            break
    
    blobs = []
    for i_o, game_offers in enumerate(js_hits['eventGroup']['offerCategories'][i_ocat]['offerSubcategoryDescriptors'][i_ocsubat]['offerSubcategory']['offers']):
        away_name, home_name = js_hits['eventGroup']['events'][i_o]['teamName1'], js_hits['eventGroup']['events'][i_o]['teamName2']
        away_nick, home_nick = ' '.join(away_name.split()[1:]), ' '.join(home_name.split()[1:])
        away_name, home_name = static_data.load_static_data.teams_nickname_to_name.get(away_nick, None), static_data.load_static_data.teams_nickname_to_name.get(home_nick, None)
        if away_name is None or home_name is None:
            print(f'Can not find the team name for nick {(away_nick, home_nick)}')
            continue
        
        for offer in game_offers:
            player_name = offer['label'].split(' Hits')[0]
            '''
            df_playdf = df_player_team_positions[df_player_team_positions.player_name == player_name]
            if len(df_playdf) == 0:
                print(f"Can not find the team for player {player_name} ({offer['label']})")
                continue
            team_name = df_playdf.iloc[0].player_team_name
            '''        
            game = find_today_game(schedule_today, away_name)
            if game is None:
                print(f"Can not find the schedule for team {away_name}")
                continue
            blob = \
            {
                'game_id': game['game_id'] if game else None,
                'game_date': game['game_date'] if game else None,
                'team_away': game['away_name'],
                'team_home': game['home_name'],
                'player_name': player_name,
                'over_odds': offer['outcomes'][0]['oddsAmerican'],
                'over_line': offer['outcomes'][0]['line'],
                'under_odds': offer['outcomes'][1]['oddsAmerican'],
                'under_line': offer['outcomes'][1]['line'],
            }
            blobs.append(blob)
            
    return pd.DataFrame(blobs).reset_index(drop=True)
