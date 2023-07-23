cd /home/sculd3/projects/mlb-props/
date >> log.txt
/home/sculd3/venvs/mlb/bin/python /home/sculd3/projects/mlb-props/fetch_today_odds_bq_update.py >> log.txt 2>&1
/home/sculd3/venvs/mlb/bin/python /home/sculd3/projects/mlb-props/predict_today_matchup.py >> log.txt 2>&1
