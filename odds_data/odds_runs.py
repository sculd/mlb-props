import odds_data.odds_over_under

def fetch_df_runs_odd_today():
    return odds_data.odds_over_under.fetch_df_odd_today(7979, 'Runs Scored', offer_label_player_name_splitter = 'Runs')


