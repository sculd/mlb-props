cd /home/sculd3/projects/mlb-props/
LOG="logs/log_$(date +%Y-%m-%dT%H:%M:%S%z).txt"
echo $LOG
echo "daily_cloud_live_update_run" >> $LOG
date >> $LOG
echo "fetch_today_odds_bq_update.py" >> $LOG
/home/sculd3/venvs/mlb/bin/python /home/sculd3/projects/mlb-props/fetch_today_odds_bq_update.py >> $LOG 2>&1
echo "predict_today_matchup.py" >> $LOG
/home/sculd3/venvs/mlb/bin/python /home/sculd3/projects/mlb-props/predict_today_matchup.py >> $LOG 2>&1
echo "read_today_prediction_odds_gcs_update.py" >> $LOG
/home/sculd3/venvs/mlb/bin/python /home/sculd3/projects/mlb-props/read_today_prediction_odds_gcs_update.py >> $LOG 2>&1
