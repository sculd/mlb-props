cd /home/sculd3/projects/mlb-props/
LOG="logs/log_daily_$(date +%Y-%m-%dT%H:%M:%S%z).txt"
echo $LOG
echo "daily_cloud_run" >> $LOG
date >> $LOG
echo "fetch_yesterday_boxscores_bq_update.py" >> $LOG
/home/sculd3/venvs/mlb/bin/python /home/sculd3/projects/mlb-props/fetch_yesterday_boxscores_bq_update.py >> $LOG 2>&1
echo "fetch_yesterday_venue_game_temperature_bq_update.py" >> $LOG
/home/sculd3/venvs/mlb/bin/python /home/sculd3/projects/mlb-props/fetch_yesterday_venue_game_temperature_bq_update.py >> $LOG 2>&1
echo "fetch_yesterday_prediction_db_update.py" >> $LOG
/home/sculd3/venvs/mlb/bin/python /home/sculd3/projects/mlb-props/fetch_yesterday_prediction_db_update.py >> $LOG 2>&1
echo "read_history_prediction_odds_gcs_update.py" >> $LOG
/home/sculd3/venvs/mlb/bin/python /home/sculd3/projects/mlb-props/read_history_prediction_odds_gcs_update.py >> $LOG 2>&1
