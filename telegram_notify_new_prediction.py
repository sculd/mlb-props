import os
if os.path.exists('credential.json'):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), 'credential.json')

import logging, sys, datetime, time


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

from dotenv import load_dotenv

load_dotenv()  # take environment variables from .env.

import notification.new_confident_predictions
import notification.telegram

count_2_hits, message_2_hits = notification.new_confident_predictions.get_new_confident_predictions_count_and_description(
    notification.new_confident_predictions.PropertyType.TWO_HITS
)

count_2_strikeouts, message_2_strikeouts = notification.new_confident_predictions.get_new_confident_predictions_count_and_description(
    notification.new_confident_predictions.PropertyType.TWO_STRIKEOUTS
)

logging.info(f'{message_2_hits=}\n{message_2_strikeouts=}')
if count_2_hits > 0 or count_2_strikeouts > 0:
    notification.telegram.post_message(f"{message_2_hits}\n{message_2_strikeouts}")
else:
    logging.info('not sending a message as no new confident predictions are found.')
