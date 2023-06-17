import pandas as pd, numpy as np

from google.cloud import bigquery

fetch_print_prefix = "\n### "

gs_bucket_name = "major-league-baseball"
gcp_project_id = "trading-290017"
bq_dataset_id = "major_league_baseball"
bq_table_id = "odds_hit_recorded"


# Construct a BigQuery client object.
bq_client = bigquery.Client()

query = """
    SELECT * 
    FROM `trading-290017.major_league_baseball.odds_hit_recorded` 
    where game_date >= "2023-04-01"
"""

def download_hit_recorded():
    query_job = bq_client.query(query)  # Make an API request.

    row_dicts = []
    for row in query_job:
        # Row values can be accessed by field name or index.
        row_dict = {k: v for k, v in row.items()}
        row_dicts.append(row_dict)


    df_odds = pd.DataFrame(row_dicts)
    df_odds['game_id'] = df_odds.game_id.astype(np.int32)

    df_odds.to_pickle('odds_data/df_odds_hit_recorded.pkl')
