# mlb sports betting forecasting model

## Forked from quantgalore.substack.com

My fork [here|https://github.com/sculd/mlb-props].

* `collect_data_run.ipynb` needs to be run inintially, to collect the player stats, game match up data, etc. which finally constructs the dataset used for training / testing.
  * `df_game_matchup_total.pkl` is the dataframe pkl that has 2011 to 2023 match up.
  * the collect_data is stored in mlb_props_data bucket of google drive.
* `model_training_run.ipynb` trains a model.
* `update_data_run.py` should run at the beginning of each day, fetching the previous date's matchup and updating all the data.
* `fetch_today_matchup_and_odds_run.py` should run at the beginning of each day, this creates matchup for today's live bet and fetches the odds for today's games.
* add this line `0 8 * * * /home/junlim/projects/mlb-props/daily_run.sh` to crontab to run it every 8am daily.
* add this line `0 10 * * * /home/junlim/projects/mlb-props/daily_live_run.sh` to crontab to run it every 10am daily.



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
test_data = df_game_matchup_total[(df_game_matchup_total.game_date > "2022-12-01")][model.common.features]
```


```python
regression_model = pycaret.classification.load_model(model.common.model_file_name)
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
    confident_prediction[['game_date', "batting_name", "batting_hit_recorded",	"prediction_score", "player_team_name", "theo_odds"]]
    l =len(confident_prediction)
    return l, confident_prediction.batting_hit_recorded.sum() / l
```


```python
score_threshold = 0.75
confident_test_prediction = test_prediction[(test_prediction["prediction_score"] >= score_threshold) & (test_prediction["prediction_label"] == 1)].sort_values(by = "prediction_score", ascending = False).drop_duplicates("batting_name")
confident_test_prediction[['game_date', "batting_name", "batting_hit_recorded",	"prediction_score", "player_team_name", "theo_odds"]]
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
      <th>batting_hit_recorded</th>
      <th>prediction_score</th>
      <th>player_team_name</th>
      <th>theo_odds</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>23933</th>
      <td>2023-05-25</td>
      <td>Randal Grichuk</td>
      <td>1</td>
      <td>0.91</td>
      <td>Colorado Rockies</td>
      <td>-1011</td>
    </tr>
    <tr>
      <th>16151</th>
      <td>2023-05-07</td>
      <td>Freddie Freeman</td>
      <td>1</td>
      <td>0.90</td>
      <td>Atlanta Braves</td>
      <td>-900</td>
    </tr>
    <tr>
      <th>7494</th>
      <td>2023-04-17</td>
      <td>Shohei Ohtani</td>
      <td>1</td>
      <td>0.88</td>
      <td>Los Angeles Angels</td>
      <td>-733</td>
    </tr>
    <tr>
      <th>9918</th>
      <td>2023-04-22</td>
      <td>Rafael Devers</td>
      <td>1</td>
      <td>0.88</td>
      <td>Boston Red Sox</td>
      <td>-733</td>
    </tr>
    <tr>
      <th>25906</th>
      <td>2023-06-03</td>
      <td>Nico Hoerner</td>
      <td>1</td>
      <td>0.88</td>
      <td>Chicago Cubs</td>
      <td>-733</td>
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
      <th>22095</th>
      <td>2023-05-23</td>
      <td>Anthony Santander</td>
      <td>1</td>
      <td>0.75</td>
      <td>Baltimore Orioles</td>
      <td>-300</td>
    </tr>
    <tr>
      <th>17950</th>
      <td>2023-05-12</td>
      <td>Marcus Semien</td>
      <td>1</td>
      <td>0.75</td>
      <td>Oakland Athletics</td>
      <td>-300</td>
    </tr>
    <tr>
      <th>15338</th>
      <td>2023-05-06</td>
      <td>C.J. Cron</td>
      <td>1</td>
      <td>0.75</td>
      <td>Colorado Rockies</td>
      <td>-300</td>
    </tr>
    <tr>
      <th>11867</th>
      <td>2023-04-27</td>
      <td>Trey Mancini</td>
      <td>1</td>
      <td>0.75</td>
      <td>Chicago Cubs</td>
      <td>-300</td>
    </tr>
    <tr>
      <th>11901</th>
      <td>2023-04-27</td>
      <td>Xander Bogaerts</td>
      <td>1</td>
      <td>0.75</td>
      <td>San Diego Padres</td>
      <td>-300</td>
    </tr>
  </tbody>
</table>
<p>61 rows × 6 columns</p>
</div>



the first is the number of the rows over the threshold. the second is the ratio of true positive among the sample.


```python
print(get_eval_profile(test_prediction, 0.6))
print(get_eval_profile(test_prediction, 0.7))
print(get_eval_profile(test_prediction, 0.75))
print(get_eval_profile(test_prediction, 0.80))
print(get_eval_profile(test_prediction, 0.85))
```

    (215, 0.6744186046511628)
    (105, 0.7238095238095238)
    (61, 0.8360655737704918)
    (29, 0.9655172413793104)
    (10, 1.0)



```python

```
