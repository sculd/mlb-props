#### Forked from quantgalore.substack.com

My fork [here|https://github.com/sculd/mlb-props].

* `collect_data_run.ipynb` needs to be run inintially, to collect the player stats, game match up data, etc. which finally constructs the dataset used for training / testing.
  * `df_game_matchup_total.pkl` is the dataframe pkl that has 2011 to 2023 match up.
  * the collect_data is stored in mlb_props_data bucket of google drive.
* `model_training_run.ipynb` trains a model.
* `update_data_run.py` should run at the beginning of each day, fetching the previous date's matchup and updating all the data.
* `fetch_today_matchup_and_odds_run.py` should run at the beginning of each day, this creates matchup for today's live bet and fetches the odds for today's games.
* add this line `0 8 * * * /home/junlim/projects/mlb-props/daily_run.sh` to crontab to run it every 8am daily.
