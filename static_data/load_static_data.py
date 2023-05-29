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

teams_nickname_to_name = {
    'Athletics': 'Oakland Athletics',
    'Angels': 'Los Angeles Angels',
    'Mariners': 'Seattle Mariners',
    'Padres': 'San Diego Padres',
    'Diamondbacks': 'Arizona Diamondbacks',
    'White Sox': 'Chicago White Sox',
    'Rockies': 'Colorado Rockies',
    'Brewers': 'Milwaukee Brewers',
    'Cubs': 'Chicago Cubs',
    'Dodgers': 'Los Angeles Dodgers',
    'Rangers': 'Texas Rangers',
    'Astros': 'Houston Astros',
    'Red Sox': 'Boston Red Sox',
    'Yankees': 'New York Yankees',
    'Phillies': 'Philadelphia Phillies',
    'Marlins': 'Miami Marlins',
    'Orioles': 'Baltimore Orioles',
    'Braves': 'Atlanta Braves',
    'Royals': 'Kansas City Royals',
    'Tigers': 'Detroit Tigers',
    'Rays': 'Tampa Bay Rays',
    'Blue Jays': 'Toronto Blue Jays',
    'Guardians': 'Cleveland Guardians',
    'Twins': 'Minnesota Twins',
    'Giants': 'San Francisco Giants',
    'Mets': 'New York Mets',
    'Nationals': 'Washington Nationals',
    'Cardinals': 'St. Louis Cardinals',
    'Pirates': 'Pittsburgh Pirates',
    'Reds': 'Cincinnati Reds',
}