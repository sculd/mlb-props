import pandas as pd

base_dir = "static_data"

Teams_and_IDs = pd.read_csv(f"{base_dir}/Teams_and_IDs.csv")
Player_Positions = pd.read_csv(f"{base_dir}/MLB_Player_Positions.csv")
Park_Data = pd.read_csv(f"{base_dir}/mlb_parks.csv")
park_venues = set(Park_Data["Venue"])

df_teams_ids = Teams_and_IDs
df_player_team_positions = Player_Positions
df_mlb_parks = Park_Data
df_park_venues = park_venues
