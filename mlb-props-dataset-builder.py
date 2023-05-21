# -*- coding: utf-8 -*-
"""
Created on Thu Apr 20 08:59:30 2023

@author: Local User
"""

import pandas as pd
import statsapi
from statsapi import player_stat_data
import requests
from datetime import datetime, timedelta
import numpy as np
import math
import sqlalchemy
import mysql.connector
import meteostat

Teams_and_IDs = pd.read_csv("Teams_and_IDs.csv")
Player_Positions = pd.read_csv("MLB_Player_Positions.csv")
Park_Data = pd.read_csv("mlb_parks.csv")

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

def Team_To_ID(team_name):

    
    team_id = Teams_and_IDs["Team_ID"][Teams_and_IDs['Team_Name'] == team_name].iloc[0]

    return team_id

def ID_To_Team(team_id):
    
    team_name = Teams_and_IDs["Team_Name"][Teams_and_IDs['Team_ID'] == team_id].iloc[0]
    
    return team_name

def Player_to_ID(player_name):
    
    player_id = Player_Positions["player_id"][Player_Positions["player_name"] == player_name]
    
    if len(player_id) >= 1:
        
        return player_id.iloc[0]
    
    else:
        
        return ""
def celsius_to_fahrenheit(celsius):
    
    return ( celsius * (9/5) ) + 32


def get_matchup(game_boxscore, side):
    opposite_side = "away" if side == "home" else "home"
    all_side_players = game_boxscore[side]["players"]
    
    side_players_list = []
    for home_player in all_side_players:
        home_player = all_side_players[home_player]
        home_player_name, home_player_id = home_player["person"]["fullName"], home_player["person"]["id"]
        side_players_list.append({"name":home_player_name, "id":home_player_id})

    side_players_dataframe = pd.DataFrame(side_players_list)
    side_batting_lineup_ids = game_boxscore[side]["battingOrder"]
    side_batters = side_players_dataframe[side_players_dataframe["id"].isin(side_batting_lineup_ids)]
    valid_historical_season = int(Game["game_date"].iloc[0][0:4]) - 1
    side_batter_stats_list = []

    for side_batter in side_batters["id"]:
        side_batter_stats_data = get_player_stat_data(side_batter, force_fetch = False)
        if side_batter_stats_data is not None:
            side_batter_stats = side_batter_stats_data["stats"]
        else:
            print(f'Error while getting {side} batter {side_batter} stat')
            return None

        if len(side_batter_stats) == 0:
            print(f'{side} batter {side_batter} stat is empty')
            return None
        
        for historical_batter_stat in side_batter_stats:
            if historical_batter_stat["season"] == str(valid_historical_season):
                season_batter_stats = historical_batter_stat["stats"]
                season_batter_stats["name"] = side_batters["name"][side_batters["id"] == side_batter].iloc[0]
                season_batter_stats["id"] = side_batters["id"][side_batters["id"] == side_batter].iloc[0]
                
                side_batter_game_day_stats = all_side_players[f"ID{side_batter}"]["stats"]["batting"]
                
                if side_batter_game_day_stats["hits"] < 1:
                    hit_recorded = 0
                elif side_batter_game_day_stats["hits"] >= 1:
                    hit_recorded = 1
                    
                season_batter_stats["hit_recorded"] = hit_recorded
                side_batter_stats_list.append(season_batter_stats)
            
    side_team_batting_stats = pd.DataFrame(side_batter_stats_list).drop_duplicates(subset = "name", keep ="last")
    
    if len(side_team_batting_stats) < 1:
        print(f'{side} side_team_batting_stats is empty for game')
        return None
    
    opposing_pitcher_name, opposing_pitcher_id = Game[f"{opposite_side}_probable_pitcher"].iloc[0], Player_to_ID(Game[f"{opposite_side}_probable_pitcher"].iloc[0])
    
    side_batting_matchup = None
    if type(opposing_pitcher_id) != type(np.int64()):
        print(f'opposing_pitcher_id {opposing_pitcher_id} is not of int64 type')
        pass
    else:
        opposing_pitcher_stats_data = get_player_stat_data(opposing_pitcher_id, force_fetch = False)
        if opposing_pitcher_stats_data is not None:            
            opposing_pitcher_stats = pd.json_normalize(opposing_pitcher_stats_data["stats"], max_level = 0)
        else:
            print(f'Error while getting {side} opssosing pitcher {opposing_pitcher_id} stat')
            return None
        
        # If there is just no data from the API for this player
        if len(opposing_pitcher_stats) == 0:
            print(f'{side} opposing pitcher {opposing_pitcher_id} stat is empty')
            pass
        else:
            valid_opposing_pitcher_season_stats = opposing_pitcher_stats[opposing_pitcher_stats["season"] == str(valid_historical_season)]["stats"].drop_duplicates(keep = "last")
        
            # If there is no historical data for last season
            if len(valid_opposing_pitcher_season_stats) == 0:
                print(f'{side} valid opposing pitcher {opposing_pitcher_id} season stat is empty')
                pass
            else:
                opposing_pitcher_season_stats = valid_opposing_pitcher_season_stats.iloc[0]
                opposing_pitcher_season_stats["name"] = opposing_pitcher_name
                opposing_pitcher_season_stats["id"] = opposing_pitcher_id
                
                opposing_pitcher_stats_dataframe = pd.DataFrame([opposing_pitcher_season_stats])
                opposing_pitcher_stats = pd.concat([opposing_pitcher_stats_dataframe]*len(side_team_batting_stats))
                side_batting_matchup = pd.concat([opposing_pitcher_stats.reset_index(drop = True).add_prefix("pitching_"), side_team_batting_stats.reset_index(drop = True).add_prefix("batting_")], axis = 1)
            
                # =============================================================================
                # End of calculating the side for the batters
                # =============================================================================
    return side_batting_matchup

# =============================================================================
# Start
# =============================================================================

yesterday = (datetime.today() - timedelta(days = 1)).strftime("%Y-%m-%d")
day_before_yesterday = (datetime.today() - timedelta(days = 2)).strftime("%Y-%m-%d")

# Schedule = statsapi.schedule(start_date = day_before_yesterday, end_date = yesterday)
Schedule = statsapi.schedule(start_date = "2023-04-01", end_date = yesterday)
Schedule_DataFrame = pd.json_normalize(Schedule)
  
game_id_list = list(Schedule_DataFrame["game_id"].drop_duplicates())

game_matchups = []

start = datetime.now()

for game_id in game_id_list:
    
    home_batting_matchup, away_batting_matchup = [], []
       
    Game = Schedule_DataFrame[Schedule_DataFrame["game_id"] == game_id]
    
    home_team_name, home_team_id = Game["home_name"].iloc[0], Game["home_id"].iloc[0]
    away_team_name, away_team_id = Game["away_name"].iloc[0], Game["away_id"].iloc[0]

    home_probable_pitcher = Game["home_probable_pitcher"].iloc[0]
    away_probable_pitcher = Game["away_probable_pitcher"].iloc[0]
    
    try:
    
        game_boxscore = statsapi.boxscore_data(game_id)
        
    except Exception:
        continue
    
    home_batting_matchup = get_matchup(game_boxscore, "home")
    away_batting_matchup = get_matchup(game_boxscore, "away")
    
    if (len(home_batting_matchup) == 0) and (len(away_batting_matchup) == 0):        
        continue    
    elif (len(home_batting_matchup) >= 1) and (len(away_batting_matchup) == 0):        
        game_matchup = home_batting_matchup.reset_index(drop = True).copy()
    elif (len(home_batting_matchup) == 0) and (len(away_batting_matchup) >= 1):        
        game_matchup = away_batting_matchup.reset_index(drop = True).copy()        
    elif (len(home_batting_matchup) >= 0) and (len(away_batting_matchup) >= 1):        
        game_matchup = pd.concat([home_batting_matchup, away_batting_matchup], axis = 0).reset_index(drop = True)

    game_matchup["game_id"] = Game["game_id"].iloc[0]
    game_matchup["game_venue"] = Game["venue_name"].iloc[0]
    game_matchup["game_date"] = Game["game_date"].iloc[0]
    game_matchup["game_datetime"] = Game["game_datetime"].iloc[0]
    
    if Game["venue_name"].isin(list(Park_Data["Venue"])).iloc[0] == False:
        continue
    
    park_lat = Park_Data["latitude"][Park_Data["Venue"] == game_matchup["game_venue"].iloc[0]].iloc[0]
    park_lon = Park_Data["longitude"][Park_Data["Venue"] == game_matchup["game_venue"].iloc[0]].iloc[0]

    point_object = meteostat.Point(lat = park_lat, lon = park_lon)

    historical_weather = meteostat.Hourly(loc = point_object, start = (pd.to_datetime(game_matchup["game_date"].iloc[0])), end = (pd.to_datetime(game_matchup["game_date"].iloc[0]) + timedelta(days = 1)), timezone = "America/Chicago").fetch().reset_index()

    pre_game_weather = historical_weather[historical_weather["time"] <= (pd.to_datetime(Game["game_datetime"].iloc[0])).tz_convert("America/Chicago")]
    last_hour_weather = pre_game_weather.tail(1).copy()
    last_hour_weather["temp_f"] = last_hour_weather["temp"].apply(celsius_to_fahrenheit).iloc[0]
    
    game_temperature = last_hour_weather["temp_f"].iloc[0]
    
    game_matchup["temp"] = game_temperature

    game_matchups.append(game_matchup)
    
end = datetime.now()
print(f"Elapsed Time: {end - start}")
    
# =============================================================================
# End    
# =============================================================================

game_matchup_dataframe = pd.concat(game_matchups).dropna()
print(f"Unique Games: {len(game_matchup_dataframe['game_id'].drop_duplicates())}")

engine = sqlalchemy.create_engine()

existing_data = pd.read_sql("SELECT * FROM baseball_historical_matchup", con = engine)
existing_data["batter_game"] = existing_data["batting_id"].astype(str)  + existing_data["game_id"].astype(str) 
game_matchup_dataframe["batter_game"] = game_matchup_dataframe["batting_id"].astype(str) + game_matchup_dataframe["game_id"].astype(str)
new_data = game_matchup_dataframe[~game_matchup_dataframe["batter_game"].isin(existing_data["batter_game"])]
new_data = new_data.drop("batter_game", axis = 1)

if len(new_data) >= 1:

    new_data.to_sql("baseball_historical_matchup", con = engine, if_exists = "append")
    
    print(len(new_data))

# with engine.connect() as conn:
#     result = conn.execute(sqlalchemy.text('DROP TABLE baseball_historical_matchup'))
