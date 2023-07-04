import odds_data.odds_over_under

def fetch_df_hits_odd_today():
    return odds_data.odds_over_under.fetch_df_odd_today(6719, 'Hits')