import pandas as pd
import datetime

year_today = int(datetime.datetime.today().strftime("%Y"))
date_today = datetime.datetime.today().strftime("%Y-%m-%d")
date_today_detail = datetime.datetime.today().strftime("%Y-%m-%dT%H:%M:%S")

fetch_print_prefix = "\n### "

# fetch yesterday's schedule
import collect_data.schedules
print(f"{fetch_print_prefix}fetch yesterday's schedule")
_schedules = collect_data.schedules._schedules
print(f"_schedules before update {len(_schedules)}")
scs_yesterday = collect_data.schedules.fetch_schedule_yesterday()
print(f"_schedules after update {len(_schedules)}")
collect_data.schedules.dump_schedule_data()

# fetch yesterday's game ids
print(f"{fetch_print_prefix}fetch yesterday's game ids")
import collect_data.game_id_lists
_game_id_lists = collect_data.game_id_lists._game_id_lists
game_id_list_year = collect_data.game_id_lists.get_game_id_list_year(year_today)
print(f"game_id_list_year before update {len(game_id_list_year)}")
collect_data.game_id_lists.add_game_id_list(year_today, list(scs_yesterday.keys()))
print(f"game_id_list_year after update {len(game_id_list_year)}")
collect_data.game_id_lists.dump_game_ids()

# fetch yesterday's boxscores
print(f"{fetch_print_prefix}fetch yesterday's boxscores")
import collect_data.boxscores
_boxscores = collect_data.boxscores._boxscores
print(f"_boxscores before update {len(_boxscores)}")
collect_data.boxscores.ingest_boxscore_game_ids(list(scs_yesterday.keys()))
print(f"_boxscores after update {len(_boxscores)}")
collect_data.boxscores.dump_boxscore_cache()

# fetch yesterday's venue_game_temperatures
print(f"{fetch_print_prefix}fetch yesterday's venue_game_temperatures")
import collect_data.venue_game_temperatures
_venue_game_temperatures = collect_data.venue_game_temperatures._venue_game_temperatures
print(f"_venue_game_temperatures before update {len(_venue_game_temperatures)}")
collect_data.venue_game_temperatures.ingest_venue_game_temperatures_for_game_id_list(sorted(list(scs_yesterday.keys())))
print(f"_venue_game_temperatures after update {len(_venue_game_temperatures)}")
collect_data.venue_game_temperatures.dump_venue_game_temperatures()

# construct yesterday's matchup
print(f"{fetch_print_prefix}construct and add yesterday's matchup")
import collect_data.game_matchup
df_game_matchup_2023 = pd.read_pickle('collect_data/df_game_matchup_2023.pkl')
print(f"df_game_matchup_2023 before update {len(df_game_matchup_2023)}")
df_game_matchup_updated = collect_data.game_matchup.get_df_game_matchup(list(scs_yesterday.keys()))
print(f"df_game_matchup_updated {len(df_game_matchup_updated)}")
df_game_matchup_2023 = pd.concat([df_game_matchup_2023, df_game_matchup_updated]).drop_duplicates(['game_id', 'batting_id', 'pitching_id'])
print(f"df_game_matchup_2023 after update {len(df_game_matchup_2023)}")
df_game_matchup_2023.to_pickle('collect_data/df_game_matchup_2023.pkl')
