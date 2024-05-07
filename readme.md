# mlb sports betting forecasting model

```
jupyter nbconvert --execute --to markdown readme.ipynb
```
is to be run to convert this notebook to a markdown file.

## Forked from quantgalore.substack.com

My fork [here|https://github.com/sculd/mlb-props].

* `collect_data_run.ipynb` needs to be run inintially, to collect the player stats, game match up data, etc. which finally constructs the dataset used for training / testing.
  * `df_game_matchup_total.pkl` is the dataframe pkl that has 2011 to 2023 match up.
  * the collect_data is stored in mlb_props_data bucket of google drive.
* `model_training_run.ipynb` trains a model.
* `update_data_run.py` should run at the beginning of each day, fetching the previous date's matchup and updating all the data.
* `fetch_today_matchup_and_odds_run.py` should run at the beginning of each day, this creates matchup for today's live bet and fetches the odds for today's games.

```
$ crontab -l
0 12-20 * * * TZ=US/Eastern /home/sculd3/projects/mlb-props/scripts/daily_cloud_live_update_run.sh
58 12-20 * * * TZ=US/Eastern /home/sculd3/projects/mlb-props/scripts/telegram_notify_new_prediction.sh
0 12 * * * TZ=US/Eastern /home/sculd3/projects/mlb-props/scripts/daily_cloud_run.sh
0 14 * * * TZ=US/Eastern /home/sculd3/projects/mlb-props/scripts/daily_cloud_live_run.sh
```

The odds fetch is hosted in the gcp vm (sandbox(2)), and updated to `trading-290017.major_league_baseball.odds_hit_recorded` table.


```python
import pycaret
import pandas as pd
import numpy as np
from pycaret import classification
from datetime import datetime
import model.common
from static_data.load_static_data import *
```


```python
collect_data_Base_dir = 'collect_data'
df_game_matchup_total = pd.read_pickle(f'{collect_data_Base_dir}/df_game_matchup_total.pkl')
test_data = df_game_matchup_total[(df_game_matchup_total.game_date > "2022-12-01")][model.common.features_1hits_recorded]
```


```python
regression_model = pycaret.classification.load_model(model.common.model_1hits_file_name)
```

    Transformation Pipeline and Model Successfully Loaded



```python
test_prediction = pycaret.classification.predict_model(data = test_data, estimator = regression_model)
test_prediction = pd.merge(test_prediction, df_player_team_positions[['player_id','player_team_name']], left_on='batting_id', right_on='player_id', how='left')
test_prediction["theo_odds"] = test_prediction["prediction_score"].apply(model.common.odds_calculator)
```


```python
def get_eval_profile(df_prediction, score_threshold):
    confident_prediction = df_prediction[(df_prediction["prediction_score"] >= score_threshold) & (df_prediction["prediction_label"] == 1)].sort_values(by = "prediction_score", ascending = False).drop_duplicates("batting_name")
    confident_prediction[['game_date', "batting_name", "batting_1hits_recorded",	"prediction_score", "player_team_name", "theo_odds"]]
    l =len(confident_prediction)
    return l, confident_prediction.batting_1hits_recorded.sum() / l
```


```python
score_threshold = 0.75
confident_test_prediction = test_prediction[(test_prediction["prediction_score"] >= score_threshold) & (test_prediction["prediction_label"] == 1)].sort_values(by = "prediction_score", ascending = False).drop_duplicates("batting_name")
confident_test_prediction[['game_date', "batting_name", "batting_1hits_recorded",	"prediction_score", "player_team_name", "theo_odds"]]
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>game_date</th>
      <th>batting_name</th>
      <th>batting_1hits_recorded</th>
      <th>prediction_score</th>
      <th>player_team_name</th>
      <th>theo_odds</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>42563</th>
      <td>2023-08-24</td>
      <td>Bo Bichette</td>
      <td>1</td>
      <td>0.8700</td>
      <td>Toronto Blue Jays</td>
      <td>-669</td>
    </tr>
    <tr>
      <th>23804</th>
      <td>2023-05-24</td>
      <td>Freddie Freeman</td>
      <td>1</td>
      <td>0.8500</td>
      <td>Los Angeles Dodgers</td>
      <td>-567</td>
    </tr>
    <tr>
      <th>35560</th>
      <td>2023-08-02</td>
      <td>Xander Bogaerts</td>
      <td>1</td>
      <td>0.8467</td>
      <td>San Diego Padres</td>
      <td>-552</td>
    </tr>
    <tr>
      <th>30734</th>
      <td>2023-06-17</td>
      <td>Mike Trout</td>
      <td>1</td>
      <td>0.8400</td>
      <td>Los Angeles Angels</td>
      <td>-525</td>
    </tr>
    <tr>
      <th>54231</th>
      <td>2023-10-22</td>
      <td>Marcus Semien</td>
      <td>1</td>
      <td>0.8300</td>
      <td>Toronto Blue Jays</td>
      <td>-488</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>33028</th>
      <td>2023-07-02</td>
      <td>Elehuris Montero</td>
      <td>1</td>
      <td>0.7500</td>
      <td>Colorado Rockies</td>
      <td>-300</td>
    </tr>
    <tr>
      <th>44703</th>
      <td>2023-08-31</td>
      <td>Miguel Cabrera</td>
      <td>0</td>
      <td>0.7500</td>
      <td>Miami Marlins</td>
      <td>-300</td>
    </tr>
    <tr>
      <th>45157</th>
      <td>2023-09-02</td>
      <td>Harrison Bader</td>
      <td>0</td>
      <td>0.7500</td>
      <td>New York Yankees</td>
      <td>-300</td>
    </tr>
    <tr>
      <th>34303</th>
      <td>2023-07-22</td>
      <td>DJ Stewart</td>
      <td>1</td>
      <td>0.7500</td>
      <td>Baltimore Orioles</td>
      <td>-300</td>
    </tr>
    <tr>
      <th>45573</th>
      <td>2023-09-03</td>
      <td>Daulton Varsho</td>
      <td>0</td>
      <td>0.7500</td>
      <td>Arizona Diamondbacks</td>
      <td>-300</td>
    </tr>
  </tbody>
</table>
<p>99 rows × 6 columns</p>
</div>



the first is the number of the rows over the threshold. the second is the ratio of true positive among the sample.


```python
print(get_eval_profile(test_prediction, 0.6))
print(get_eval_profile(test_prediction, 0.7))
print(get_eval_profile(test_prediction, 0.75))
print(get_eval_profile(test_prediction, 0.80))
print(get_eval_profile(test_prediction, 0.85))
```

    (424, 0.5660377358490566)
    (212, 0.5990566037735849)
    (99, 0.5858585858585859)
    (31, 0.7741935483870968)
    (2, 1.0)



```python

```
