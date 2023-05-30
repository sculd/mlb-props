import pandas as pd
import datetime

year_today = int(datetime.datetime.today().strftime("%Y"))
date_today = datetime.datetime.today().strftime("%Y-%m-%d")
date_today_detail = datetime.datetime.today().strftime("%Y-%m-%dT%H:%M:%S")

fetch_print_prefix = "\n### "

# fetch today's schedule
import collect_data.schedules
print(f"{fetch_print_prefix}fetch today's schedule")
_schedules = collect_data.schedules._schedules
print(f"_schedules before update {len(_schedules)}")
scs_today = collect_data.schedules.fetch_schedule_today()
print(f"_schedules after update {len(_schedules)} (will not dump as today's boxscore is not Final)")

# fetch today's boxscores
print(f"{fetch_print_prefix}fetch today's boxscores")
import collect_data.boxscores
_boxscores = collect_data.boxscores._boxscores
print(f"_boxscores before update {len(_boxscores)}")
boxscore_for_ids = {}
collect_data.boxscores.ingest_boxscore_game_ids(list(scs_today.keys()))
print(f"_boxscores after update {len(_boxscores)} (will not dump as today's boxscore is not Final)")

# fetch today's venue_game_temperatures
print(f"{fetch_print_prefix}fetch today's venue_game_temperatures")
import collect_data.venue_game_temperatures
_venue_game_temperatures = collect_data.venue_game_temperatures._venue_game_temperatures
print(f"_venue_game_temperatures before update {len(_venue_game_temperatures)}")
collect_data.venue_game_temperatures.ingest_venue_game_temperatures_for_game_id_list(sorted(list(scs_today.keys())), schs=scs_today)
print(f"_venue_game_temperatures after update {len(_venue_game_temperatures)} (will not dump as today's temp is based on weather forecasting)")

# construct today's matchup for live betting
print(f"{fetch_print_prefix}construct today's matchup for live betting")
import collect_data.game_matchup
df_game_matchup_updated = collect_data.game_matchup.get_df_game_matchup(list(scs_today.keys()))
print(f"df_game_matchup_updated {len(df_game_matchup_updated)}")
df_game_matchup_updated.to_pickle(f'collect_data/df_live_game_matchup_{date_today}.pkl')
