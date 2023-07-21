import pandas as pd, numpy as np

from google.cloud import bigquery

fetch_print_prefix = "\n### "

gs_bucket_name = "major-league-baseball"
gcp_project_id = "trading-290017"
bq_dataset_id = "major_league_baseball"
bq_table_id = "odds_hit_recorded"


# Construct a BigQuery client object.
bq_client = bigquery.Client()

_clause_property_format = 'property = "{property}"'
_clause_property_all = "TRUE"

_clause_date_between_format = 'game_date >= "{start_date_str}" AND game_date <= "{end_date_str}"'
_clause_date_2023 = 'game_date >= "2023-04-01"'

_query = """
    SELECT * 
    FROM `trading-290017.major_league_baseball.odds_batter_prop`
    WHERE {clause_property} 
    AND {clause_date}
"""

def read_df_property_query(query):
    print(f'[read_df_property_query] running querry\n{query}')
    query_job = bq_client.query(query)  # Make an API request.

    row_dicts = []
    for row in query_job:
        # Row values can be accessed by field name or index.
        row_dict = {k: v for k, v in row.items()}
        row_dicts.append(row_dict)

    df_odds = pd.DataFrame(row_dicts)
    if len(df_odds) == 0:
        return df_odds

    df_odds['game_date'] = pd.to_datetime(df_odds['game_date'])
    df_odds['game_id'] = df_odds.game_id.astype(np.int32)
    return df_odds

def read_df_property_between(start_date_str, end_date_str, property = 'all'):
    if property == 'all':
        clause_property = _clause_property_all
    else:
        clause_property = _clause_property_format.format(property=property)

    clause_date = _clause_date_between_format.format(start_date_str=start_date_str, end_date_str=end_date_str)
    query_formatted = _query.format(clause_property=clause_property, clause_date = clause_date)
    return read_df_property_query(query_formatted)

def read_df_property_date(date_str, property = 'all'):
    return read_df_property_between(date_str, date_str, property=property)

def read_df_property_2023(property = 'all'):
    if property == 'all':
        clause_property = _clause_property_all
    else:
        clause_property = _clause_property_format.format(property=property)

    query_formatted = _query.format(clause_property=clause_property, clause_date = _clause_date_2023)
    return read_df_property_query(query_formatted)

def download_property_2023(property = 'all'):
    df_odds = read_df_property_2023(property = property)
    df_odds.to_pickle(f'odds_data/df_odds_{property}.pkl')
