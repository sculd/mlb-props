import datetime, os, json

if os.path.exists('credential.json'):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), 'credential.json')


import update_data.prediction
update_data.prediction.update_prediction_db_yesterday()





