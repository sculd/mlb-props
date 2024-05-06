import os
if os.path.exists('credential.json'):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), 'credential.json')


from google.cloud import bigquery
from enum import auto, Enum

import datetime, pytz
import pandas as pd, numpy as np

gcp_project_id = "trading-290017"
bq_dataset_id = "major_league_baseball"
bq_table_id = "prediction_batter_prop"
bq_table_full_id = f'{gcp_project_id}.{bq_dataset_id}.{bq_table_id}'

_bq_client = bigquery.Client()

_2_hits_query_param = {'odds_property': 'Hits', 'prediction_property': 'batting_2hits_recorded',
                        'line_clause': 'AND under_line = 1.5',
                        'prediction_score_threshold': 0.75, 'prediction_label': 0}

_2_strikeouts_query_param = {'odds_property': 'Strikeouts', 'prediction_property': 'batting_2strikeOuts_recorded',
                        'line_clause': 'AND under_line = 1.5',
                        'prediction_score_threshold': 0.75, 'prediction_label': 0}


_query_format = '''
WITH LATEST AS (
  SELECT game_id, game_date, player_name, property, over_line, under_line, MAX(ingested_datetime) as max_ingested_datetime
  FROM `trading-290017.major_league_baseball.odds_batter_prop` 
  WHERE TRUE
  and game_date >= '{date}'
  and property = '{odds_property}'
  {line_clause}
  GROUP BY game_id, game_date, player_name, property, over_line, under_line
  ORDER BY game_date DESC
), 
LO AS (
  SELECT O.*
  FROM `trading-290017.major_league_baseball.odds_batter_prop` AS O JOIN LATEST ON 
    O.game_id = LATEST.game_id
    AND O.game_date = LATEST.game_date
    AND O.player_name = LATEST.player_name 
    AND O.property = LATEST.property 
    AND O.over_line = LATEST.over_line 
    AND O.under_line = LATEST.under_line 
    AND O.ingested_datetime = LATEST.max_ingested_datetime
), 
PO AS (
  SELECT P.*, TIMESTAMP_TRUNC(P.ingestion_datetime, HOUR) as ingestion_datetime_hourly, LO.over_odds, LO.over_line, LO.under_odds, LO.under_line
  FROM `trading-290017.major_league_baseball.live_prediction_batter_prop` as P JOIN LO ON
    P.batting_name = LO.player_name
    AND P.date = LO.game_date

  WHERE TRUE
  and date >= '{date}'
  and property_name = '{prediction_property}'
  and prediction_score >= {prediction_score_threshold}
  and prediction_label = {prediction_label}
  ORDER BY date DESC, property_name, batting_name
)

SELECT date, ingestion_datetime_hourly, property_name, COUNT(*) AS cnt
#SELECT *
FROM PO
GROUP BY date, ingestion_datetime_hourly, property_name
ORDER BY date DESC, ingestion_datetime_hourly DESC
'''

class PropertyType(Enum):
    TWO_HITS = auto()
    TWO_STRIKEOUTS = auto()


def get_recent_confident_prediction_dits(property_type: PropertyType):
    if property_type is PropertyType.TWO_HITS:
        query_param = _2_hits_query_param
    elif property_type is PropertyType.TWO_STRIKEOUTS:
        query_param = _2_strikeouts_query_param
    else:
        query_param = {}

    date_str_today = datetime.datetime.now(pytz.timezone('US/Eastern')).strftime("%Y-%m-%d")
    query_str = _query_format.format(
        date = date_str_today, **query_param,
    )
    print(query_str)

    query_job = _bq_client.query(query_str)
    latest_row_dict = None
    second_latest_row_dict = None
    for row in query_job:
        # Row values can be accessed by field name or index.
        row_dict = {k: v for k, v in row.items()}
        second_latest_row_dict = row_dict if second_latest_row_dict is None and latest_row_dict is not None else second_latest_row_dict
        latest_row_dict = row_dict if latest_row_dict is None else latest_row_dict

    return latest_row_dict, second_latest_row_dict


def get_new_confident_predictions_description(property_type: PropertyType) -> str:
    latest_row_dict, second_latest_row_dict = get_recent_confident_prediction_dits(property_type)
    prev_confident_predictions = second_latest_row_dict['cnt'] if second_latest_row_dict is not None else 0
    latest_confident_predictions = latest_row_dict['cnt'] if latest_row_dict is not None else 0
    new_confident_predictions = latest_confident_predictions - prev_confident_predictions

    when = latest_row_dict['ingestion_datetime_hourly'] if latest_row_dict is not None else '(unknown time)'
    return f"{property_type}: {new_confident_predictions} new confident predictions since {when}."

