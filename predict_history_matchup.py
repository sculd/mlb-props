import datetime, pytz
import os

if os.path.exists('credential.json'):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), 'credential.json')

import update_data.prediction

update_data.prediction.update_prediction_db_between("2023-04-01", "2023-12-01")


