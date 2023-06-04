

import collect_data.game_matchup
df_game_matchup_updated = collect_data.game_matchup.get_game_matchup(717951)


print(df_game_matchup_updated)

columns = ["pitching_gamesPlayed", "pitching_runs", "pitching_strikeOuts", "pitching_hits", "pitching_id", "batting_name"] + \
          ["batting_gamesPlayed",  "batting_runs",  "batting_strikeOuts",  "batting_hits",  "batting_rbi", "batting_id", "pitching_name", "batting_hit_recorded"] + \
          ["temp", "game_venue", 'game_date']
print(df_game_matchup_updated[columns])


