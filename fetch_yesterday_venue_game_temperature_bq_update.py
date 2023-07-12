import datetime, os, json

if os.path.exists('credential.json'):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), 'credential.json')


import update_data.venue_game_temperatures
venue_game_temperatures = update_data.venue_game_temperatures.upload_venue_game_temperatures_to_gcs_yesterday()
print(f'venue_game_temperatures: {len(venue_game_temperatures)}')

