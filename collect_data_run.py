import pandas as pd, numpy as np
import statsapi
from statsapi import player_stat_data
import requests, json
from static_data.load_static_data import *

All_Teams_Data = []

for team_id in Teams_and_IDs['Team_ID']:
    roster_url = f"https://statsapi.mlb.com/api/v1/teams/{team_id}/roster?rosterType=allTime"
    response = requests.get(roster_url)
    roster_data = response.json()['roster']
    for player in roster_data:
        Player_Name = player['person']['fullName']
        Player_ID = player['person']['id']
        Player_Dictionary = {"Team_ID":team_id,"Player Name":Player_Name, "Player ID":Player_ID}
        All_Teams_Data.append(Player_Dictionary)

All_Teams_DataFrame = pd.DataFrame(All_Teams_Data)
# almost the same could be achieve this way.
All_Teams_DataFrame_ = Player_Positions.drop_duplicates(['player_name', 'player_id', 'player_team_id'])[['player_team_id', 'player_name', 'player_id']]

'''
Here is the dependency and accordingly the right order of runs.
* schedules
* game_id_lists
* boxscores: game_id_lists
* player_stat
* venue_game_temperature: schedules
* game_matchup: schedules, boxscores, venue_game_temperature
'''

import collect_data.schedules
_schedules = collect_data.schedules._schedules

import collect_data.game_id_lists
_game_id_lists = collect_data.game_id_lists._game_id_lists

game_id_list_2011_2022 = sum([collect_data.game_id_lists.get_game_id_list_year(y) for y in range(2011, 2023)], [])
game_id_list_2023 = collect_data.game_id_lists.get_game_id_list_year(2023)

import collect_data.boxscores
_boxscores = collect_data.boxscores._boxscores

import collect_data.player_stat
_player_stats = collect_data.player_stat._player_stats

import collect_data.venue_game_temperatures
_venue_game_temperatures = collect_data.venue_game_temperatures._venue_game_temperatures

import collect_data.game_matchup

df_game_matchup_total = collect_data.game_matchup.get_df_game_matchup(game_id_list_2011_2022 + game_id_list_2023)

print(f"Unique Games: {len(df_game_matchup_total['game_id'].drop_duplicates())}")
len(df_game_matchup_total)
df_game_matchup_total.to_pickle('collect_data/df_game_matchup_total.pkl')

df_game_matchup_2023 = collect_data.game_matchup.get_df_game_matchup(game_id_list_2023)
print(f"Unique Games: {len(df_game_matchup_2023['game_id'].drop_duplicates())}")
len(df_game_matchup_2023)
df_game_matchup_2023.to_pickle('collect_data/df_game_matchup_2023.pkl')
