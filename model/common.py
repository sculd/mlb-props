Features = ['pitching_gamesPlayed', 'pitching_gamesStarted', 'pitching_groundOuts',
       'pitching_airOuts', 'pitching_runs', 'pitching_doubles',
       'pitching_triples', 'pitching_homeRuns', 'pitching_strikeOuts',
       'pitching_baseOnBalls', 'pitching_intentionalWalks', 'pitching_hits',
       'pitching_hitByPitch', 'pitching_avg', 'pitching_atBats',
       'pitching_obp', 'pitching_slg', 'pitching_ops',
       'pitching_caughtStealing', 'pitching_stolenBases', 'pitching_groundIntoDoublePlay',
       'pitching_numberOfPitches',
       'pitching_wins', 'pitching_losses', 'pitching_saves',
       'pitching_saveOpportunities', 'pitching_holds', 'pitching_blownSaves',
       'pitching_earnedRuns', 'pitching_battersFaced',
       'pitching_outs', 'pitching_gamesPitched', 'pitching_completeGames',
       'pitching_shutouts', 'pitching_strikes', 'pitching_strikePercentage',
       'pitching_hitBatsmen', 'pitching_balks', 'pitching_wildPitches',
       'pitching_pickoffs', 'pitching_totalBases','pitching_gamesFinished',
       'pitching_inheritedRunners', 'pitching_inheritedRunnersScored',
       'pitching_catchersInterference', 'pitching_sacBunts',
       'pitching_sacFlies', 'pitching_name', 'pitching_id',
       'batting_gamesPlayed', 'batting_groundOuts', 'batting_airOuts',
       'batting_runs', 'batting_doubles', 'batting_triples',
       'batting_homeRuns', 'batting_strikeOuts', 'batting_baseOnBalls',
       'batting_intentionalWalks', 'batting_hits', 'batting_hitByPitch',
       'batting_avg', 'batting_atBats', 'batting_obp', 'batting_slg',
       'batting_ops', 'batting_caughtStealing', 'batting_stolenBases', 'batting_groundIntoDoublePlay',
       'batting_numberOfPitches', 'batting_plateAppearances',
       'batting_totalBases', 'batting_rbi', 'batting_leftOnBase',
       'batting_sacBunts', 'batting_sacFlies', 'batting_catchersInterference',
       'batting_name', 'batting_id',
       'batting_hit_recorded', 'game_id', 'game_venue', 'game_date', 'game_year']

#Data[["batting_avg","batting_obp","batting_slg","batting_ops","pitching_strikePercentage", "pitching_avg" , "pitching_era", "pitching_obp", "pitching_slg", "pitching_ops"]] = Data[["batting_avg","batting_obp","batting_slg","batting_ops","pitching_strikePercentage", "pitching_avg" , "pitching_era", "pitching_obp", "pitching_slg", "pitching_ops"]].replace([".---", "-.--"], np.nan).astype(float)

features = \
    ["pitching_gamesPlayed", "pitching_runs", "pitching_strikeOuts", "pitching_hits", "pitching_id", "batting_name"] + \
    ["batting_gamesPlayed",  "batting_runs",  "batting_strikeOuts",  "batting_hits",  "batting_rbi", "batting_id", "pitching_name", "batting_hit_recorded"] + \
    ["temp", "game_venue", 'game_date', "game_year"]

Hit_Real_Features = features

categorical_features = ["game_venue"] + ["batting_name"] + ["pitching_name"]

ignore_features = ["pitching_id", "batting_id", 'game_date', "game_year"]

#Exclude_Variables = ["Unnamed: 0","pitching_numberOfPitches","pitching_outs","pitching_strikes","pitching_strikePercentage","pitching_totalBases","pitching_baseOnBalls","pitching_hitByPitch","pitching_atBats",       'batting_groundOuts','batting_hitByPitch', 'batting_caughtStealing','batting_stolenBases','batting_plateAppearances',"pitching_game_id","batting_game_id",'batting_leftOnBase', 'batting_sacBunts', 'batting_sacFlies',"batting_numberOfPitches","batting_totalBases","batting_baseOnBalls", "batting_doubles", "batting_triples","batting_homeRuns","batting_airOuts","batting_atBats", "batting_rbi"]

# I am not sure why Real_Features is besides Hit_Real_Featuress
Real_Features = \
    ["pitching_gamesPlayed","pitching_runs", "pitching_era", "pitching_strikeOuts", "pitching_hits", "pitching_id", "pitching_name"] + \
    ["batting_gamesPlayed", "batting_runs", "batting_strikeOuts", "batting_hits", "batting_rbi", "batting_id", "batting_name"] + \
    ["temp", "game_id", "game_venue","game_date"]

model_base_dir = 'model'
#hit_save_to_file_string = f"{datetime.today().strftime('%Y%m%d')}_" + "batter_hit_lr_model"
model_file_name = f"{model_base_dir}/batter_hit_lr_model"

def odds_calculator(probability):
    return round(-100 / ((1/probability)-1))

def juiced_odds_calculator(probability):
    return odds_calculator(probabiliby) - 15

def odds_to_probability(odds):
    odds = float(odds)
    if odds >= 0.0:
        return 1.0 / (1.0 + (odds / 100.0))
    else:
        return 1.0 / (1.0 + (100.0 / abs(odds)))
