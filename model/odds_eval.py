import pandas as pd, numpy as np
import pycaret.classification

import model.common
from static_data.load_static_data import df_player_team_positions

prediction_columns = ['prediction_label', 'prediction_score', 'theo_odds']

def df_prediction_add_odd(df_matchup, regression_model):
    '''
    make prediction over the given matchup and add `theo_odds` column with basic player, team columns.
    '''
    df_matchup = df_matchup.loc[:,~df_matchup.columns.duplicated()].copy()
    df_prediction = pycaret.classification.predict_model(data = df_matchup, estimator = regression_model)
    df_prediction = pd.merge(df_prediction, df_player_team_positions[['player_id','player_team_name']], left_on='batting_id', right_on='player_id', how='left')
    df_prediction["theo_odds"] = df_prediction["prediction_score"].apply(model.common.odds_calculator)
    return df_prediction

def merge_df_prediction_over_odds(df_prediction, df_over_odds, target_column, target_line):
    '''
    merge the prediction with the gamebook's odds data.

    target_line: e.g. 1.0
    '''
    df_over_odds = df_over_odds.copy()
    df_over_odds["over_prob"] = df_over_odds["over_odds"].apply(model.common.odds_to_probability)
    df_prediction_odds = df_prediction.set_index(['game_id', 'batting_name']).join(\
        df_over_odds.rename(columns={'player_name': 'batting_name'}).set_index(['game_id', 'batting_name']), lsuffix='', rsuffix='_odds').reset_index()
    df_prediction_odds = df_prediction_odds[df_prediction_odds.over_line < target_line]
    return df_prediction_odds

def get_df_confident_prediction_odds(df_prediction_odds, target_column, score_threshold = 0.70):
    df_prediction_odds = df_prediction_odds.loc[:,~df_prediction_odds.columns.duplicated()].copy()
    df_confident_prediction_odds = df_prediction_odds.drop_duplicates(["game_id", "batting_name"])
    df_confident_prediction_odds = df_confident_prediction_odds[df_confident_prediction_odds["prediction_score"] >= score_threshold]
    # for some reason, the prediction_label should be separatedly checked. higher score does not always lead to prediction label. (maybe the score stands for both labels).
    df_confident_prediction_odds = df_confident_prediction_odds[df_confident_prediction_odds["prediction_label"] == 1]
    df_confident_prediction_odds = df_confident_prediction_odds.sort_values('prediction_score')
    hits = df_confident_prediction_odds[target_column].sum()
    l = len(df_confident_prediction_odds)
    print(f'hit recorded ratio: {1.0 * hits / l} ({hits} out of {l})')
    #df_confident_prediction_odds = df_confident_prediction_odds.loc[:,~df_confident_prediction_odds.columns.duplicated()].copy()
    ideal_reward = np.add(1.0, np.divide(100.0, np.abs(df_confident_prediction_odds.over_odds)))
    profit = np.sum(np.multiply(df_confident_prediction_odds[target_column], ideal_reward)) - l
    print(f'profit: {profit}')
    
    return df_confident_prediction_odds[['game_date', 'team_away', 'team_home', 'batting_name', target_column] + prediction_columns + ['over_prob', 'over_odds', 'over_line']]

def get_df_advantageous_prediction_odds(df_prediction_odds, target_column, prediction_diff_threshold = 0.05, score_threshold = 0.60):
    df_prediction_odds_ = df_prediction_odds.loc[:,~df_prediction_odds.columns.duplicated()].copy()
    df_prediction_odds_  = df_prediction_odds_.copy()
    df_prediction_odds_  = df_prediction_odds_.drop_duplicates("batting_name")
    df_prediction_odds_['prediction_diff'] = df_prediction_odds_['prediction_score'] - df_prediction_odds_['over_prob']
    df_advantageous_prediction_odds = df_prediction_odds_
    df_advantageous_prediction_odds = df_advantageous_prediction_odds[df_advantageous_prediction_odds["prediction_score"] >= score_threshold]
    df_advantageous_prediction_odds = df_advantageous_prediction_odds[df_advantageous_prediction_odds["prediction_label"] == 1]
    df_advantageous_prediction_odds = df_advantageous_prediction_odds[df_advantageous_prediction_odds["prediction_diff"] >= prediction_diff_threshold]
    df_advantageous_prediction_odds['prediction_score_plus_diff'] = df_advantageous_prediction_odds['prediction_score'] + df_advantageous_prediction_odds['prediction_diff']
    df_advantageous_prediction_odds = df_advantageous_prediction_odds.sort_values('prediction_score_plus_diff')
    hits = df_advantageous_prediction_odds[target_column].sum()
    l = len(df_advantageous_prediction_odds)
    print(f'hit recorded ratio: {1.0 * hits / l} ({hits} out of {l})')
    #df_advantageous_prediction_odds = df_advantageous_prediction_odds.loc[:,~df_advantageous_prediction_odds.columns.duplicated()].copy()
    ideal_reward = np.add(1.0, np.divide(100.0, np.abs(df_advantageous_prediction_odds.over_odds)))
    profit = np.sum(np.multiply(df_advantageous_prediction_odds[target_column], ideal_reward)) - l
    print(f'profit: {profit}')
    
    return df_advantageous_prediction_odds[['game_date', 'team_away', 'team_home', 'batting_name', "prediction_diff", target_column] + prediction_columns + ['over_prob', 'over_odds', 'over_line']]
