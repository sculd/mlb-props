import os
if os.path.exists('credential.json'):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), 'credential.json')


from dotenv import load_dotenv

load_dotenv()  # take environment variables from .env.

import notification.new_confident_predictions
import notification.telegram

notification.telegram.post_message(
    notification.new_confident_predictions.get_new_confident_predictions_description(
        notification.new_confident_predictions.PropertyType.TWO_HITS
    ))


notification.telegram.post_message(
    notification.new_confident_predictions.get_new_confident_predictions_description(
        notification.new_confident_predictions.PropertyType.TWO_STRIKEOUTS
    ))


