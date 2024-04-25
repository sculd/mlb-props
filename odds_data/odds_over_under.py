import pandas as pd
import numpy as np
import requests
import statsapi
from datetime import datetime

from static_data.load_static_data import *
import static_data.load_static_data


def fetch_odds_json(subcategory_id):
    #url = f'https://sportsbook-us-ny.draftkings.com//sites/US-NY-SB/api/v5/eventgroups/84240/categories/743/subcategories/{subcategory_id}?format=json'
    url = f'https://sportsbook.draftkings.com/sites/US-NJ-SB/api/v5/eventgroups/84240/categories/743/subcategories/{subcategory_id}?format=json'

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

def fetch_df_odd_today(subcategory_id, subcategory_name, offer_label_player_name_splitter = None):
    jd_odds = fetch_odds_json(subcategory_id)
    schedule_today = get_schedule_today()

    i_ocat = 0
    for i, cat in enumerate(jd_odds['eventGroup']['offerCategories']):
        if cat['name'] == 'Batter Props':
            i_ocat = i
            break
    
    i_ocsubat = 0
    for i, subcat in enumerate(jd_odds['eventGroup']['offerCategories'][i_ocat]['offerSubcategoryDescriptors']):
        if subcat['name'] == subcategory_name:
            i_ocsubat = i
            break
    
    blobs = []
    for i_o, game_offers in enumerate(jd_odds['eventGroup']['offerCategories'][i_ocat]['offerSubcategoryDescriptors'][i_ocsubat]['offerSubcategory']['offers']):
        away_name, home_name = jd_odds['eventGroup']['events'][i_o]['teamName1'], jd_odds['eventGroup']['events'][i_o]['teamName2']
        away_nick, home_nick = ' '.join(away_name.split()[1:]), ' '.join(home_name.split()[1:])
        away_name, home_name = static_data.load_static_data.teams_nickname_to_name.get(away_nick, None), static_data.load_static_data.teams_nickname_to_name.get(home_nick, None)
        if away_name is None or home_name is None:
            print(f'Can not find the team name for nick {(away_nick, home_nick)}')
            continue

        offer_label_player_name_splitter = subcategory_name if offer_label_player_name_splitter is None else offer_label_player_name_splitter
        for offer in game_offers:
            player_name = offer['label'].split(f' {offer_label_player_name_splitter}')[0]
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
                'property': subcategory_name,
                'over_odds': offer['outcomes'][0]['oddsAmerican'],
                'over_line': offer['outcomes'][0]['line'],
                'under_odds': offer['outcomes'][1]['oddsAmerican'],
                'under_line': offer['outcomes'][1]['line'],
            }
            blobs.append(blob)
            
    return pd.DataFrame(blobs).reset_index(drop=True)
