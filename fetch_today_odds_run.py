import pandas as pd
import datetime

year_today = int(datetime.datetime.today().strftime("%Y"))
date_today = datetime.datetime.today().strftime("%Y-%m-%d")
date_today_detail = datetime.datetime.today().strftime("%Y-%m-%dT%H:%M:%S")

fetch_print_prefix = "\n### "

# fetch today's odds
print(f"{fetch_print_prefix}fetch today's odds")
import odds_data.odds_hits
df_odds_hits = odds_data.odds_hits.fetch_df_hits_odd_today()
print(f'df_odds_hits {df_odds_hits}')
df_odds_hits.to_pickle(f"odds_data/df_odds_hits_{date_today}.pkl")
df_odds_hits.to_pickle(f"odds_data/df_odds_hits_{date_today_detail}.pkl")
