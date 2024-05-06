USER_NAME=sculd3 # sculd3
VENV_NAME=mlb # mlb
cd /home/${USER_NAME}/projects/mlb-props/
LOG="logs/log_$(date +%Y-%m-%dT%H:%M:%S%z).txt"
echo $LOG
echo "telegram_notify_new_prediction" >> $LOG
date >> $LOG
echo "telegram_notify_new_prediction.py" >> $LOG
/home/${USER_NAME}/venvs/${VENV_NAME}/bin/python /home/${USER_NAME}/projects/mlb-props/telegram_notify_new_prediction.py >> $LOG 2>&1
