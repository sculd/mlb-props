cd /home/sculd3/projects/mlb-props/
/home/sculd3/venvs/mlb/bin/python /home/sculd3/projects/mlb-props/fetch_today_odds_bq_update.py >> log.txt
/home/sculd3/venvs/mlb/bin/python /home/sculd3/projects/mlb-props/fetch_yesterday_boxscores_bq_update.py >> log.txt
/home/sculd3/venvs/mlb/bin/python /home/sculd3/projects/mlb-props/fetch_yesterday_venue_game_temperature_bq_update.py >> log.txt
/home/sculd3/venvs/mlb/bin/python /home/sculd3/projects/mlb-props/fetch_yesterday_prediction_db_update.py >> log.txt
