import update_data.odds_hits
import datetime

df_odds_hits = update_data.odds_hits.fetch_df_hits_odd_today()

date_today = datetime.datetime.today().strftime("%Y-%m-%d")
date_today_detail = datetime.datetime.today().strftime("%Y-%m-%dT%H:%M:%S")

df_odds_hits.to_pickle(f"df_odds_hits_{date_today}.pkl")
df_odds_hits.to_pickle(f"df_odds_hits_{date_today_detail}.pkl")
