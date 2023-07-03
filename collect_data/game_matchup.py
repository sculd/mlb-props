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


def get_side_batter_matchup(game_id, side, batter_player_id, player_boxscore_stats_batting, player_boxscore_season_stats_batting, force_fetch=False):
    game = _schedules[game_id]
    if game is None:
        print(f'Failed to get schedule detail for game_id: {game_id}')
        return None

    batter_stats_data = get_player_stat_data(batter_player_id, group="hitting", force_fetch=force_fetch)
    if batter_stats_data is None:
        print(f'Error while getting {side} batter {batter_player_id} stat')
        return None

    side_batter_stats = batter_stats_data["stats"]
    if len(side_batter_stats) == 0:
        # print(f'{side} batter {side_batter} stat is empty')
        return None

    season_last_year_str = str(int(game["game_date"][0:4]) - 1)
    df_better = df_player_team_positions[df_player_team_positions.player_id == batter_player_id]
    if len(df_better) == 0:
        print(f'better name can not be found for {batter_player_id}')
        return None

    batter_player_name = df_better.iloc[0].player_name

    season_batter_stats = {}
    season_batter_stats["name"] = batter_player_name
    season_batter_stats["id"] = batter_player_id
    # learning targets
    season_batter_stats["1hits_recorded"] = 1 if player_boxscore_stats_batting["hits"] >= 1 else 0
    season_batter_stats["2hits_recorded"] = 1 if player_boxscore_stats_batting["hits"] >= 2 else 0
    season_batter_stats["1homeRuns_recorded"] = 1 if player_boxscore_stats_batting["homeRuns"] >= 1 else 0
    season_batter_stats["1strikeOuts_recorded"] = 1 if player_boxscore_stats_batting["strikeOuts"] >= 1 else 0
    season_batter_stats["2strikeOuts_recorded"] = 1 if player_boxscore_stats_batting["strikeOuts"] >= 2 else 0
    season_batter_stats["1runs_recorded"] = 1 if player_boxscore_stats_batting["runs"] >= 1 else 0
    season_batter_stats["2runs_recorded"] = 1 if player_boxscore_stats_batting["runs"] >= 2 else 0
    season_batter_stats["1stolenBases_recorded"] = 1 if player_boxscore_stats_batting["stolenBases"] >= 1 else 0
    season_batter_stats["2stolenBases_recorded"] = 1 if player_boxscore_stats_batting["stolenBases"] >= 2 else 0

    # current season stats should come from boxscore not from player stat as otherwise it would be look-ahead bias.
    season_batter_stats['cur_season_avg'] = float(player_boxscore_season_stats_batting['avg'])
    season_batter_stats['cur_season_obp'] = float(player_boxscore_season_stats_batting['obp'])
    season_batter_stats['cur_season_slg'] = float(player_boxscore_season_stats_batting['slg'])
    season_batter_stats['cur_season_ops'] = float(player_boxscore_season_stats_batting['ops'])

    # last year season stats
    last_year_stat_found = False
    for historical_batter_stat in side_batter_stats:
        if historical_batter_stat["season"] == season_last_year_str:
            season_batter_stats.update(historical_batter_stat["stats"])
            season_batter_stats["runs_per_game"] = 1. * season_batter_stats["runs"] / season_batter_stats["gamesPlayed"]
            season_batter_stats["strikeOuts_per_game"] = 1. * season_batter_stats["strikeOuts"] / season_batter_stats["gamesPlayed"]
            season_batter_stats["hits_per_game"] = 1. * season_batter_stats["hits"] / season_batter_stats["gamesPlayed"]
            last_year_stat_found = True

    return season_batter_stats if last_year_stat_found else None


def get_side_pitcher_matchup_for_game(game_id, side, force_fetch=False):
    game = _schedules[game_id]
    if game is None:
        print(f'Failed to get schedule detail for game_id: {game_id}')
        return None

    pitcher_name = game[f"{side}_probable_pitcher"]
    pitcher_id = player_name_to_id(pitcher_name)
    if type(pitcher_id) != type(np.int64()):
        print(f'pitcher_id {pitcher_id} can not be found for pitcher {pitcher_name}')
        return None

    pitcher_stats_data = get_player_stat_data(pitcher_id, group="pitching", force_fetch=force_fetch)
    if pitcher_stats_data is None:
        print(f'Error while getting {side} pitcher {pitcher_id} stat')
        return None

    pitcher_stats_stats = pitcher_stats_data["stats"]
    season_last_year_str = str(int(game["game_date"][0:4]) - 1)
    season_pitcher_stats = {}
    season_pitcher_stats["name"] = pitcher_name
    season_pitcher_stats["id"] = pitcher_id

    game_boxscore = get_boxscore_data(game_id)
    if game_boxscore is None:
        print(f'Failed to get box score for game_id: {game_id}')
        return None

    if f'ID{pitcher_id}' not in game_boxscore[side]['players']:
        print(f'ID{pitcher_id} not in game_boxscore[{side}]["players"]')
        return None

    # current season stats should come from boxscore not from player stat as otherwise it would be look-ahead bias.
    player_boxscore_season_stats_pitching = game_boxscore[side]['players'][f'ID{pitcher_id}']['seasonStats']['pitching']
    season_pitcher_stats['cur_season_obp'] = float(player_boxscore_season_stats_pitching['obp'])
    season_pitcher_stats['cur_hits_per_pitch'] = player_boxscore_season_stats_pitching['hits'] / player_boxscore_season_stats_pitching['numberOfPitches'] if player_boxscore_season_stats_pitching['numberOfPitches'] > 0 else 0
    season_pitcher_stats['cur_runs_per_pitch'] = player_boxscore_season_stats_pitching['runs'] / player_boxscore_season_stats_pitching['numberOfPitches'] if player_boxscore_season_stats_pitching['numberOfPitches'] > 0 else 0
    season_pitcher_stats['cur_homeRuns_per_pitch'] = player_boxscore_season_stats_pitching['homeRuns'] / player_boxscore_season_stats_pitching['numberOfPitches'] if player_boxscore_season_stats_pitching['numberOfPitches'] > 0 else 0
    season_pitcher_stats['cur_strikeOuts_per_pitch'] = player_boxscore_season_stats_pitching['strikeOuts'] / player_boxscore_season_stats_pitching['numberOfPitches'] if player_boxscore_season_stats_pitching['numberOfPitches'] > 0 else 0

    last_year_stat_found = False
    for historical_pitcher_stat in pitcher_stats_stats:
        if historical_pitcher_stat["season"] == season_last_year_str:
            season_pitcher_stats.update(historical_pitcher_stat["stats"])
            season_pitcher_stats["runs_per_game"] = 1. * season_pitcher_stats["runs"] / season_pitcher_stats["gamesPlayed"]
            season_pitcher_stats["strikeOuts_per_game"] = 1. * season_pitcher_stats["strikeOuts"] / season_pitcher_stats["gamesPlayed"]
            season_pitcher_stats["hits_per_game"] = 1. * season_pitcher_stats["hits"] / season_pitcher_stats["gamesPlayed"]
            last_year_stat_found = True

    return season_pitcher_stats if last_year_stat_found else None

# side for home or away
def get_df_side_matchup(game_id, side, force_fetch = False):
    game_boxscore = get_boxscore_data(game_id)
    if game_boxscore is None:
        print(f'Failed to get box score for game_id: {game_id}')
        return None

    all_players = game_boxscore[side]["players"]
    side_players_list = []
    for player in all_players:
        player = all_players[player]
        player_name, player_id = player["person"]["fullName"], player["person"]["id"]
        side_players_list.append({"name":player_name, "id":player_id})

    side_players_dataframe = pd.DataFrame(side_players_list)
    side_batting_lineup_ids = game_boxscore[side]["battingOrder"]

    side_batters = side_players_dataframe[side_players_dataframe["id"].isin(side_batting_lineup_ids)]
    side_batter_stats_list = []
    for side_batter_id in side_batters["id"]:
        # player_boxscore_stats_batting, player_boxscore_season_stats_batting
        side_batter_matchup = get_side_batter_matchup(
            game_id, side, side_batter_id,
            game_boxscore[side]['players'][f'ID{side_batter_id}']['stats']['batting'],
            game_boxscore[side]['players'][f'ID{side_batter_id}']['seasonStats']['batting'],
            force_fetch=force_fetch)
        if side_batter_matchup is None:
            continue
        side_batter_stats_list.append(side_batter_matchup)

    df_side_team_batting_stats = pd.DataFrame(side_batter_stats_list).drop_duplicates(subset = "name", keep ="last")
    if len(df_side_team_batting_stats) < 1:
        print(f'{side} side_team_batting_stats is empty for game {game_id}')
        return None

    opposite_side = "away" if side == "home" else "home"
    opposing_pitcher_stats = get_side_pitcher_matchup_for_game(game_id, opposite_side, force_fetch=force_fetch)
    if opposing_pitcher_stats is None:
        return None

    df_opposing_pitcher_stats_filled = pd.concat([pd.DataFrame([opposing_pitcher_stats])] * len(df_side_team_batting_stats))
    df_side_matchup = pd.concat([df_opposing_pitcher_stats_filled.reset_index(drop=True).add_prefix("pitching_"),
                                 df_side_team_batting_stats.reset_index(drop=True).add_prefix("batting_")], axis=1)

    return df_side_matchup

# home + away plus misc data like temperature
def get_df_game_matchup_for_game_id(game_id):
    game = _schedules[game_id]
    if game["venue_name"] not in park_venues:
        # likely a training game
        return None

    home_batting_matchup = get_df_side_matchup(game_id, "home")
    away_batting_matchup = get_df_side_matchup(game_id, "away")
    
    home_batting_matchup = home_batting_matchup if home_batting_matchup is not None else []
    away_batting_matchup = away_batting_matchup if away_batting_matchup is not None else []

    game_matchup = None
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
        _ = get_df_side_matchup(game_id, "home", force_fetch = force_fetch)
        _ = get_df_side_matchup(game_id, "away", force_fetch = force_fetch)

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


def get_df_game_matchup_batch(game_id_list, get_df_game_matchup_batch_result):
    df_game_matchups = []
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

        home_batting_matchup = get_df_side_matchup(game_id, "home")
        away_batting_matchup = get_df_side_matchup(game_id, "away")

        if home_batting_matchup is None and away_batting_matchup is None:
            #print(f'None both home_batting_matchup and away_batting_matchup for game_id {game_id}')
            cnt_null_matchups += 1

        df_game_matchup = get_df_game_matchup_for_game_id(game_id)
        if df_game_matchup is None:
            continue

        df_game_matchups.append(df_game_matchup)

    print(f'cnt_null_matchups: {cnt_null_matchups}')

    if len(df_game_matchups) == 0:
        return None

    df_game_matchup_batch = pd.concat(df_game_matchups).reset_index(drop=True) # [Hit_Real_Features].dropna()
    df_game_matchup_batch['game_date'] = pd.to_datetime(df_game_matchup_batch['game_date'])
    df_game_matchup_batch['game_datetime'] = pd.to_datetime(df_game_matchup_batch['game_datetime'])
    df_game_matchup_batch['game_year'] = df_game_matchup_batch.game_date.dt.to_period('y')

    get_df_game_matchup_batch_result.append(df_game_matchup_batch)
    return df_game_matchup_batch

def get_df_game_matchup_(game_id_list):
    print(f'get_df_game_matchup_concurrent game_ids: {len(game_id_list)}')
    global _boxscores
    global _boxscores_lock

    n_ths = 8
    game_id_list_splits = np.array_split(game_id_list, n_ths)
    ths = []
    df_game_matchup_batch_result = [[] for _ in range(n_ths)]
    # single thread is too slong (1+ hour per year) and the bottlenecked in network
    for i, game_id_list_split in enumerate(game_id_list_splits):
        print(f'th {i}')
        th = threading.Thread(target=get_df_game_matchup_batch, args=(game_id_list_split, df_game_matchup_batch_result[i]))
        th.start()
        ths.append(th)

    for th in ths:
        th.join()

    print(f'done get_df_game_matchup_concurrent {len(game_id_list)}')
    dfs = [dflisted[0] for dflisted in df_game_matchup_batch_result if len(dflisted) > 0]
    if len(dfs) == 0:
        return None
    return pd.concat(dfs)

# this one reads up the cached data to construct a df
def get_df_game_matchup(game_id_list):
    df_game_matchups = []
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
    
        home_batting_matchup = get_df_side_matchup(game_id, "home")
        away_batting_matchup = get_df_side_matchup(game_id, "away")
        
        if home_batting_matchup is None and away_batting_matchup is None:
            #print(f'None both home_batting_matchup and away_batting_matchup for game_id {game_id}')
            cnt_null_matchups += 1
    
        df_game_matchup = get_df_game_matchup_for_game_id(game_id)
        if df_game_matchup is None:
            continue
        
        df_game_matchups.append(df_game_matchup)
    
    print(f'cnt_null_matchups: {cnt_null_matchups}')
    
    df_game_matchup = pd.concat(df_game_matchups).reset_index(drop=True) # [Hit_Real_Features].dropna()
    df_game_matchup['game_date'] = pd.to_datetime(df_game_matchup['game_date'])
    df_game_matchup['game_datetime'] = pd.to_datetime(df_game_matchup['game_datetime'])
    df_game_matchup['game_year'] = df_game_matchup.game_date.dt.to_period('y')

    return df_game_matchup