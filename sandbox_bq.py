import datetime, os
import pandas as pd, numpy as np

if os.path.exists('credential.json'):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), 'credential.json')


import odds_data.query_bq_odds

odds_data.query_bq_odds.download_hit_recorded()
