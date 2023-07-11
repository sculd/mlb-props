import datetime, os, json

if os.path.exists('credential.json'):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), 'credential.json')


import update_data.boxscores
boxscores = update_data.boxscores.upload_boxscores_to_gcs_2days_prior()
print(f'boxscores: {len(boxscores)}')

