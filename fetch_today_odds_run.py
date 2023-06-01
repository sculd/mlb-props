import pandas as pd
import datetime, os

year_today = int(datetime.datetime.today().strftime("%Y"))
date_today = datetime.datetime.today().strftime("%Y-%m-%d")
date_today_detail = datetime.datetime.today().strftime("%Y-%m-%dT%H:%M:%S")

fetch_print_prefix = "\n### "

# fetch today's odds
print(f"{fetch_print_prefix}fetch today's odds")
import odds_data.odds_hits
df_odds_hits = odds_data.odds_hits.fetch_df_hits_odd_today()
print(f'df_odds_hits {df_odds_hits}')
file_today = f"odds_data/df_odds_hits_{date_today}.pkl"

if_write_file_today = True
if os.path.exists(file_today):
    if_write_file_today = len(df_odds_hits) > len(pd.read_pickle(file_today))
    if not if_write_file_today:
        print(f'not going to write {file_today} as the existing one is already up to date.')

if if_write_file_today:
    df_odds_hits.to_pickle(file_today)
df_odds_hits.to_pickle(f"odds_data/df_odds_hits_{date_today_detail}.pkl")
