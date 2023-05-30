import pandas as pd, numpy as np
import statsapi
from statsapi import player_stat_data
import requests
from datetime import datetime, timedelta
import numpy as np
import math
import pickle
import threading

from collect_data.common import *
from collect_data.schedules import _schedules
from collect_data.boxscores import *
from collect_data.player_stat import *
from collect_data.venue_game_temperatures import *

base_dir = "collect_data"

from static_data.load_static_data import *

def player_name_to_id(player_name):
    player_id = Player_Positions["player_id"][Player_Positions["player_name"] == player_name]
    if len(player_id) >= 1:
        return player_id.iloc[0]
    else:
        return ""

# side for home or away
def get_side_matchup(game_id, side, force_fetch = False):
    game_boxscore = get_boxscore_data(game_id)
    if game_boxscore is None:
        print(f'Failed to get box score for game_id: {game_id}')
        return None

    game = _schedules[game_id]
    if game is None:
        print(f'Failed to get schedule detail for game_id: {game_id}')
        return None
    
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
    valid_historical_season = int(game["game_date"][0:4]) - 1
    side_batter_stats_list = []

    for side_batter in side_batters["id"]:
        side_batter_stats_data = get_player_stat_data(side_batter, group="hitting", force_fetch = force_fetch)
        if side_batter_stats_data is not None:
            side_batter_stats = side_batter_stats_data["stats"]
        else:
            print(f'Error while getting {side} batter {side_batter} stat')
            return None

        if len(side_batter_stats) == 0:
            #print(f'{side} batter {side_batter} stat is empty')
            return None

        seasons = set([stat["season"] for stat in side_batter_stats])
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

                if side_batter_game_day_stats["homeRuns"] < 1:
                    hr_recorded = 0
                elif side_batter_game_day_stats["homeRuns"] >= 1:
                    hr_recorded = 1

                season_batter_stats["homeRuns_recorded"] = hr_recorded

                side_batter_stats_list.append(season_batter_stats)
            
    side_team_batting_stats = pd.DataFrame(side_batter_stats_list).drop_duplicates(subset = "name", keep ="last")
    
    if len(side_team_batting_stats) < 1:
        print(f'{side} side_team_batting_stats is empty for game {game_id}, available seasons: {seasons}')
        return None
    
    opposing_pitcher_name, opposing_pitcher_id = game[f"{opposite_side}_probable_pitcher"], player_name_to_id(game[f"{opposite_side}_probable_pitcher"])
    
    side_batting_matchup = None
    if type(opposing_pitcher_id) != type(np.int64()):
        print(f'opposing_pitcher_id {opposing_pitcher_id} is not of int64 type')
        pass
    else:
        opposing_pitcher_stats_data = get_player_stat_data(opposing_pitcher_id, group="pitching", force_fetch = force_fetch)
        if opposing_pitcher_stats_data is not None:            
            opposing_pitcher_stats = pd.json_normalize(opposing_pitcher_stats_data["stats"], max_level = 0)
        else:
            print(f'Error while getting {side} opssosing pitcher {opposing_pitcher_id} stat')
            return None
        
        # If there is just no data from the API for this player
        if len(opposing_pitcher_stats) == 0:
            #print(f'{side} opposing pitcher {opposing_pitcher_id} stat is empty')
            pass
        else:
            valid_opposing_pitcher_season_stats = opposing_pitcher_stats[opposing_pitcher_stats["season"] == str(valid_historical_season)]["stats"].drop_duplicates(keep = "last")
        
            # If there is no historical data for last season
            if len(valid_opposing_pitcher_season_stats) == 0:
                print(f'{side} valid opposing pitcher {opposing_pitcher_id} season {valid_historical_season} stat is empty')
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

# home + away plus misc data like temperature
def get_game_matchup(game_id):
    game = _schedules[game_id]
    if game["venue_name"] not in park_venues:
        # likely a training game
        return None

    game_boxscore = get_boxscore_data(game_id)
    if game_boxscore is None:
        print(f'Failed to get box score for game_id: {game_id}')
        return None

    home_batting_matchup = get_side_matchup(game_id, "home")
    away_batting_matchup = get_side_matchup(game_id, "away")
    
    home_batting_matchup = home_batting_matchup if home_batting_matchup is not None else []
    away_batting_matchup = away_batting_matchup if away_batting_matchup is not None else []

    all_home_players = game_boxscore["home"]["players"]
    all_away_players = game_boxscore["away"]["players"]

    if (len(home_batting_matchup) == 0) and (len(away_batting_matchup) == 0):
        #print(f'None both home_batting_matchup and away_batting_matchup for game_id {game_id}')
        return None
    elif (len(home_batting_matchup) >= 1) and (len(away_batting_matchup) == 0):
        #print(f'None away_batting_matchup for game_id {game_id}')
        game_matchup = home_batting_matchup.reset_index(drop = True).copy()
    elif (len(home_batting_matchup) == 0) and (len(away_batting_matchup) >= 1):
        #print(f'None home_batting_matchup for game_id {game_id}')
        game_matchup = away_batting_matchup.reset_index(drop = True).copy()
    elif (len(home_batting_matchup) >= 0) and (len(away_batting_matchup) >= 1):
        game_matchup = pd.concat([home_batting_matchup, away_batting_matchup], axis = 0).reset_index(drop = True)

    game_matchup["game_id"] = game["game_id"]
    game_matchup["game_venue"] = game["venue_name"]
    game_matchup["game_date"] = game["game_date"]
    game_matchup["game_datetime"] = game["game_datetime"]

    park_lat = Park_Data["latitude"][Park_Data["Venue"] == game["venue_name"]].iloc[0]
    park_lon = Park_Data["longitude"][Park_Data["Venue"] == game["venue_name"]].iloc[0]

    game_temperature = get_venue_game_temperatures(park_lat, park_lon, game["game_date"], game["game_datetime"])
    game_matchup["temp"] = game_temperature

    return game_matchup

def ingest_matchup_batch(game_ids, force_fetch = False):
    print(f'{game_ids[:2]} total {len(game_ids)} (force_fetch: {force_fetch})')

    for i, game_id in enumerate(game_ids):
        if i % 100 == 0:
            print(f'{i} of {game_ids[:2]} total {len(game_ids)}')
        home_batting_matchup = get_side_matchup(game_id, "home", force_fetch = force_fetch)
        away_batting_matchup = get_side_matchup(game_id, "away", force_fetch = force_fetch)

# injest to fill up the cache without constructing a df
def ingest_matchup_year(year, force_fetch = False):
    print(f'ingest_matchup_year year: {year}')
    game_id_list = get_game_id_list_year(year)

    game_id_list_splits = np.array_split(game_id_list, 8)
    ths = []
    for i, game_id_list_split in enumerate(game_id_list_splits):
        print(f'th {i}')
        th = threading.Thread(target=ingest_matchup_batch, args=(game_id_list_split, ), kwargs={"force_fetch": force_fetch})
        th.start()
        ths.append(th)

    for th in ths:
        th.join()

    print(f'done ingest_matchup_year year: {year}')    

# this one reads up the cached data to construct a df
def get_df_game_matchup(game_id_list):
    game_matchups = []
    cnt_null_matchups = 0
    for i, game_id in enumerate(game_id_list):
        if i % 1000 == 0:
            print(f'processing {i} out of {len(game_id_list)} game_id {game_id}, cnt_null_matchups: {cnt_null_matchups}')
    
        game = _schedules[game_id]
        if game["venue_name"] not in park_venues:
            # likely a training game
            continue
    
        game_boxscore = get_boxscore_data(game_id)
        if game_boxscore is None:
            print(f'Failed to get box score for game_id: {game_id}')
            continue
    
        home_batting_matchup = get_side_matchup(game_id, "home")
        away_batting_matchup = get_side_matchup(game_id, "away")
        
        if home_batting_matchup is None and away_batting_matchup is None:
            #print(f'None both home_batting_matchup and away_batting_matchup for game_id {game_id}')
            cnt_null_matchups += 1
    
        game_matchup = get_game_matchup(game_id)
        if game_matchup is None:
            continue
        
        game_matchups.append(game_matchup)
    
    print(f'cnt_null_matchups: {cnt_null_matchups}')
    
    df_game_matchup = pd.concat(game_matchups).reset_index(drop=True) # [Hit_Real_Features].dropna()
    df_game_matchup['game_date'] = pd.to_datetime(df_game_matchup['game_date'])
    df_game_matchup['game_datetime'] = pd.to_datetime(df_game_matchup['game_datetime'])
    df_game_matchup['game_year'] = df_game_matchup.game_date.dt.to_period('y')

    return df_game_matchup