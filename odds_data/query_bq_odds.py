import pandas as pd, numpy as np
import datetime
import pytz

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

def _get_date_clause_for_year(year):
    return f'game_date >= "{year}-04-01" AND game_date <= "{year}-12-01"'

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

    # this column name has a typo
    columns = [c for c in df_odds.columns if c != 'ingested_datettime']
    df_odds = df_odds[columns]

    df_odds['game_date'] = pd.to_datetime(df_odds['game_date'])
    df_odds['game_id'] = df_odds.game_id.astype(np.int32)
    df_odds = df_odds.sort_values(["game_date", "team_away", "team_home", "player_name", "ingested_datetime"]).drop_duplicates(["game_id", "team_away", "team_home", "player_name", "property"])
    return df_odds

def read_df_property_between(start_date_str, end_date_str, property = 'all'):
    if property == 'all':
        clause_property = _clause_property_all
    else:
        clause_property = _clause_property_format.format(property=property)

    clause_date = _clause_date_between_format.format(start_date_str=start_date_str, end_date_str=end_date_str)
    query_formatted = _query.format(clause_property=clause_property, clause_date = clause_date)
    return read_df_property_query(query_formatted)

def read_df_property_today(property = 'all'):
    date_str_today = datetime.datetime.now(pytz.timezone('US/Eastern')).strftime("%Y-%m-%d")
    return read_df_property_between(date_str_today, date_str_today, property=property)

def read_df_property_date(date_str, property = 'all'):
    return read_df_property_between(date_str, date_str, property=property)

def read_df_property_year(year, property = 'all'):
    if property == 'all':
        clause_property = _clause_property_all
    else:
        clause_property = _clause_property_format.format(property=property)

    query_formatted = _query.format(clause_property=clause_property, clause_date = _get_date_clause_for_year(year))
    return read_df_property_query(query_formatted)

def download_property_today(property = 'all'):
    df_odds = read_df_property_today(property = property)
    df_odds.to_pickle(f'odds_data/df_odds_today_{property}.pkl')
    return df_odds

def download_property_between(start_date_str, end_date_str, property = 'all'):
    df_odds = read_df_property_between(start_date_str, end_date_str, property = property)
    df_odds.to_pickle(f'odds_data/df_odds_{start_date_str}_{end_date_str}_{property}.pkl')
    return df_odds

def download_df_property_year(year, property = 'all'):
    df_odds = read_df_property_year(year, property = property)
    df_odds.to_pickle(f'odds_data/df_odds_{year}_{property}.pkl')
    return df_odds

