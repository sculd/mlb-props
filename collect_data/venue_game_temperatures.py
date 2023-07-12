import meteostat
import math

from collect_data.common import *
from collect_data.schedules import _schedules
from collect_data.game_id_lists import *
from static_data.load_static_data import *

base_dir = "collect_data"
_pkl_file_path = f'{base_dir}/venue_game_temperatures.pkl'

_venue_game_temperatures = pickle.load(open(_pkl_file_path, 'rb'))
#_venue_game_temperatures = {}

def celsius_to_fahrenheit(celsius):
    return ( celsius * (9/5) ) + 32 

def get_venue_game_temperatures(lat, lon, game_date, game_datetime):
    global _venue_game_temperatures

    key = str((lat, lon, game_datetime,))
    game_temperature = None
    if key not in _venue_game_temperatures:
        historical_weather = meteostat.Hourly(loc = meteostat.Point(lat = lat, lon = lon), start = (pd.to_datetime(game_date)), end = (pd.to_datetime(game_date) + timedelta(days = 1)), timezone = "America/Chicago").fetch().reset_index()
        pre_game_weather = historical_weather[historical_weather["time"] <= (pd.to_datetime(game_datetime)).tz_convert("America/Chicago")]
        last_hour_weather = pre_game_weather.tail(1).copy()
        game_temperature = celsius_to_fahrenheit(last_hour_weather["temp"].iloc[0])
    else:
        game_temperature = _venue_game_temperatures[key]

    return game_temperature

def ingest_venue_game_temperatures_for_game_id_list(game_id_list, schs = None):
    if schs is None:
        schs = _schedules
    cnt_invalid_venue = 0
    invalid_venues = set()
    venue_game_temperatures = {}
    for i, game_id in enumerate(game_id_list):
        if i % 1000 == 0:
            print(f'processing {i} out of {len(game_id_list)} game_id {game_id} cnt_invalid_venue: {cnt_invalid_venue}, invalid_venues: {len(invalid_venues)}')
        game = schs[game_id]
    
        if game["venue_name"] not in park_venues:
            #print(f'game["venue_name"] {game["venue_name"]} not valid')
            cnt_invalid_venue += 1
            invalid_venues.add(game["venue_name"])
            continue
    
        df_venue = df_mlb_parks[df_mlb_parks.Venue == game["venue_name"]].iloc[0]
        park_lat, park_lon = df_venue.latitude, df_venue.longitude
        game_temperature = get_venue_game_temperatures(park_lat, park_lon, game["game_date"], game["game_datetime"])
        key = str((park_lat, park_lon, game["game_datetime"],))
        if np.isnan(game_temperature) or math.isnan(game_temperature):
            continue
    
        _venue_game_temperatures[key] = game_temperature
        venue_game_temperatures[key] = game_temperature
    
    print(f'cnt_invalid_venue {cnt_invalid_venue}, invalid_venues: {len(invalid_venues)}')
    return venue_game_temperatures

def dump_venue_game_temperatures():
    pickle.dump(_venue_game_temperatures, open(_pkl_file_path, 'wb'))

