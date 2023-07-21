import os

if os.path.exists('credential.json'):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), 'credential.json')

import update_data.live_prediction

update_data.live_prediction.update_prediction_db_today()
