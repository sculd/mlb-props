import pandas as pd, numpy as np
import pycaret
import tabulate
from pycaret.classification import plot_model

from static_data.load_static_data import df_player_team_positions


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

target_1hits_recorded = "batting_1hits_recorded"
features_1hits_recorded = \
    ["pitching_gamesPlayed", "pitching_runs_per_game", "pitching_strikeOuts_per_game", "pitching_hits_per_game"] + \
    ["pitching_id", "batting_name"] + \
    ["batting_gamesPlayed",  "batting_runs_per_game",  "batting_strikeOuts_per_game",  "batting_hits_per_game"] + \
    ["batting_rbi", "batting_id", "pitching_name"] + \
    ["pitching_cur_hits_per_pitch", "pitching_cur_strikeOuts_per_pitch"] + \
    ["batting_cur_season_avg"] + \
    ["temp", "game_venue", 'game_date', "game_year"] + [target_1hits_recorded]


target_2hits_recorded = "batting_2hits_recorded"
features_2hits_recorded = \
    ["pitching_gamesPlayed", "pitching_runs_per_game", "pitching_strikeOuts_per_game", "pitching_hits_per_game"] + \
    ["pitching_id", "batting_name"] + \
    ["batting_gamesPlayed",  "batting_runs_per_game",  "batting_strikeOuts_per_game",  "batting_hits_per_game"] + \
    ["batting_rbi", "batting_id", "pitching_name"] + \
    ["pitching_cur_hits_per_pitch", "pitching_cur_strikeOuts_per_pitch"] + \
    ["batting_cur_season_avg"] + \
    ["temp", "game_venue", 'game_date', "game_year"] + [target_2hits_recorded]

target_1homeruns_recorded = "batting_1homeRuns_recorded"
features_1homeruns_recorded = \
    ["pitching_gamesPlayed", "pitching_runs_per_game", "pitching_strikeOuts_per_game", "pitching_hits_per_game"] + \
    ["pitching_id", "batting_name"] + \
    ["batting_gamesPlayed",  "batting_runs_per_game",  "batting_strikeOuts_per_game",  "batting_hits_per_game"] + \
    ["batting_rbi", "batting_id", "pitching_name"] + \
    ["pitching_cur_hits_per_pitch", "pitching_cur_strikeOuts_per_pitch"] + \
    ["batting_cur_season_avg"] + \
    ["temp", "game_venue", 'game_date', "game_year"] + [target_1homeruns_recorded]

target_1hstrikeouts_recorded = "batting_1strikeOuts_recorded"
features_1hstrikeouts_recorded = \
    ["pitching_gamesPlayed", "pitching_runs_per_game", "pitching_strikeOuts_per_game", "pitching_hits_per_game"] + \
    ["pitching_id", "batting_name"] + \
    ["batting_gamesPlayed",  "batting_runs_per_game",  "batting_strikeOuts_per_game",  "batting_hits_per_game"] + \
    ["batting_rbi", "batting_id", "pitching_name"] + \
    ["pitching_cur_hits_per_pitch", "pitching_cur_strikeOuts_per_pitch"] + \
    ["batting_cur_season_avg"] + \
    ["temp", "game_venue", 'game_date', "game_year"] + [target_1hstrikeouts_recorded]

target_2hstrikeouts_recorded = "batting_2strikeOuts_recorded"
features_2hstrikeouts_recorded = \
    ["pitching_gamesPlayed", "pitching_runs_per_game", "pitching_strikeOuts_per_game", "pitching_hits_per_game"] + \
    ["pitching_id", "batting_name"] + \
    ["batting_gamesPlayed",  "batting_runs_per_game",  "batting_strikeOuts_per_game",  "batting_hits_per_game"] + \
    ["batting_rbi", "batting_id", "pitching_name"] + \
    ["pitching_cur_hits_per_pitch", "pitching_cur_strikeOuts_per_pitch"] + \
    ["batting_cur_season_avg"] + \
    ["temp", "game_venue", 'game_date', "game_year"] + [target_2hstrikeouts_recorded]

target_1runs_recorded = "batting_1runs_recorded"
features_1runs_recorded = \
    ["pitching_gamesPlayed", "pitching_runs_per_game", "pitching_strikeOuts_per_game", "pitching_hits_per_game"] + \
    ["pitching_id", "batting_name"] + \
    ["batting_gamesPlayed",  "batting_runs_per_game",  "batting_strikeOuts_per_game",  "batting_hits_per_game"] + \
    ["batting_rbi", "batting_id", "pitching_name"] + \
    ["pitching_cur_hits_per_pitch", "pitching_cur_strikeOuts_per_pitch"] + \
    ["batting_cur_season_avg"] + \
    ["temp", "game_venue", 'game_date', "game_year"] + [target_1runs_recorded]

target_2runs_recorded = "batting_2runs_recorded"
features_2runs_recorded = \
    ["pitching_gamesPlayed", "pitching_runs_per_game", "pitching_strikeOuts_per_game", "pitching_hits_per_game"] + \
    ["pitching_id", "batting_name"] + \
    ["batting_gamesPlayed",  "batting_runs_per_game",  "batting_strikeOuts_per_game",  "batting_hits_per_game"] + \
    ["batting_rbi", "batting_id", "pitching_name"] + \
    ["pitching_cur_hits_per_pitch", "pitching_cur_strikeOuts_per_pitch"] + \
    ["batting_cur_season_avg"] + \
    ["temp", "game_venue", 'game_date', "game_year"] + [target_2runs_recorded]

target_1stolenbases_recorded = "batting_1stolenBases_recorded"
features_1stolenbases_recorded = \
    ["pitching_gamesPlayed", "pitching_runs_per_game", "pitching_strikeOuts_per_game", "pitching_hits_per_game"] + \
    ["pitching_id", "batting_name"] + \
    ["batting_gamesPlayed",  "batting_runs_per_game",  "batting_strikeOuts_per_game",  "batting_hits_per_game"] + \
    ["batting_rbi", "batting_id", "pitching_name"] + \
    ["pitching_cur_hits_per_pitch", "pitching_cur_strikeOuts_per_pitch"] + \
    ["batting_cur_season_avg"] + \
    ["temp", "game_venue", 'game_date', "game_year"] + [target_1stolenbases_recorded]

target_2stolenbases_recorded = "batting_2stolenBases_recorded"
features_2stolenbases_recorded = \
    ["pitching_gamesPlayed", "pitching_runs_per_game", "pitching_strikeOuts_per_game", "pitching_hits_per_game"] + \
    ["pitching_id", "batting_name"] + \
    ["batting_gamesPlayed",  "batting_runs_per_game",  "batting_strikeOuts_per_game",  "batting_hits_per_game"] + \
    ["batting_rbi", "batting_id", "pitching_name"] + \
    ["pitching_cur_hits_per_pitch", "pitching_cur_strikeOuts_per_pitch"] + \
    ["batting_cur_season_avg"] + \
    ["temp", "game_venue", 'game_date', "game_year"] + [target_2stolenbases_recorded]



#    ["pitching_cur_season_obp", "pitching_cur_runs_per_pitch", "pitching_cur_homeRuns_per_pitch"] + \
#    ["batting_cur_season_obp", "batting_cur_season_slg", "batting_cur_season_ops"] + \
#    ["pitching_runs", "pitching_strikeOuts", "pitching_hits"] + \
#    ["batting_runs",  "batting_strikeOuts",  "batting_hits"] + \

categorical_features = ["game_venue"] + ["batting_name"] + ["pitching_name"]

ignore_features = ["pitching_id", "batting_id", 'game_date', "game_year"]

#Exclude_Variables = ["Unnamed: 0","pitching_numberOfPitches","pitching_outs","pitching_strikes","pitching_strikePercentage","pitching_totalBases","pitching_baseOnBalls","pitching_hitByPitch","pitching_atBats",       'batting_groundOuts','batting_hitByPitch', 'batting_caughtStealing','batting_stolenBases','batting_plateAppearances',"pitching_game_id","batting_game_id",'batting_leftOnBase', 'batting_sacBunts', 'batting_sacFlies',"batting_numberOfPitches","batting_totalBases","batting_baseOnBalls", "batting_doubles", "batting_triples","batting_homeRuns","batting_airOuts","batting_atBats", "batting_rbi"]

model_base_dir = 'model'
#hit_save_to_file_string = f"{datetime.today().strftime('%Y%m%d')}_" + "batter_hit_regression_model"
model_1hits_file_name = f"{model_base_dir}/batter_1hits_regression_model"
model_2hits_file_name = f"{model_base_dir}/batter_2hits_regression_model"
model_1homeruns_file_name = f"{model_base_dir}/batter_1homeruns_regression_model"
model_1hstrikeouts_file_name = f"{model_base_dir}/batter_1hstrikeouts_regression_model"
model_2hstrikeouts_file_name = f"{model_base_dir}/batter_2hstrikeouts_regression_model"
model_1runs_file_name = f"{model_base_dir}/batter_1runs_regression_model"
model_2runs_file_name = f"{model_base_dir}/batter_2runs_regression_model"
model_1stolenbases_file_name = f"{model_base_dir}/batter_1stolenbases_regression_model"
model_2stolenbases_file_name = f"{model_base_dir}/batter_2stolenbases_regression_model"



def odds_calculator(probability):
    if probability >= 1:
        return -10000
    return round(-100 / ((1/probability)-1))

def probability_to_odds(probability):
    if probability >= 1:
        return -10000
    return round(-100 / ((1/probability)-1))

def juiced_odds_calculator(probability):
    return odds_calculator(probability) - 15

def odds_to_probability(odds):
    odds = float(odds)
    if odds >= 0.0:
        return 1.0 / (1.0 + (odds / 100.0))
    else:
        return 1.0 / (1.0 + (100.0 / abs(odds)))

        
# training

def create_models(train_data, target_column_name):
    classification_setup = pycaret.classification.setup(
        data = train_data, target = target_column_name, train_size = .70, use_gpu = False, 
        categorical_features = ['game_venue'], ignore_features = ignore_features + ['batting_name', 'pitching_name'])
    
    # `remove_outlier` produces `worse` performance for high score cases.
    regression_model_ada = classification_setup.create_model("ada", probability_threshold = 0.50)
    plot_model(regression_model_ada, plot = 'auc', scale = 0.6)
    regression_model_lightgbm = classification_setup.create_model("lightgbm", probability_threshold = 0.50)
    plot_model(regression_model_lightgbm, plot = 'auc', scale = 0.6)
    regression_model_gbc = classification_setup.create_model("gbc", probability_threshold = 0.50)
    plot_model(regression_model_gbc, plot = 'auc', scale = 0.6)
    regression_model_rf = classification_setup.create_model("rf", probability_threshold = 0.50)
    plot_model(regression_model_rf, plot = 'auc', scale = 0.6)
    return regression_model_ada, regression_model_lightgbm, regression_model_gbc, regression_model_rf



def predict_and_odds(df_data, regression_model):
    df_prediction = pycaret.classification.predict_model(data = df_data, estimator = regression_model)
    df_prediction = pd.merge(df_prediction, df_player_team_positions[['player_id','player_team_name']], left_on='batting_id', right_on='player_id', how='left')
    df_prediction["theo_odds"] = df_prediction["prediction_score"].apply(odds_calculator)
    return df_prediction

def get_eval_profile(df_prediction, score_threshold, target_column):
    confident_prediction = df_prediction.drop_duplicates("batting_name")
    confident_prediction = confident_prediction[confident_prediction["prediction_score"] >= score_threshold]
    # for some reason, the prediction_label should be separatedly checked. higher score does not always lead to prediction label. (maybe the score stands for both labels).
    confident_prediction = confident_prediction[confident_prediction["prediction_label"] == 1]
    l = len(confident_prediction)
    return l, round(confident_prediction[target_column].sum() / l, 2)

def batch_predict_and_odds(data, models):
    return [predict_and_odds(data, model) for model in models]

def evaluate_predictions(model_labels, ths, target_column):
    data = [["model"] + ths] + \
        [[model_label[1]] + [get_eval_profile(model_label[0], th, target_column) for th in ths] for model_label in model_labels]
    table = tabulate.tabulate(data, tablefmt='html')
    return table
