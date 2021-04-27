
import json
import pickle
import io
from datetime import timedelta, datetime

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

#from ncdc.ncdc_helpers import hello


# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'Airlaunch',
    'retries': 5,
    'retry_delay': timedelta(seconds=5),
    'email': ['airflow@airlaunch.ch'],
    'email_on_failure': True,
    'email_on_retry': False,
}

@dag(schedule_interval="@daily", start_date=datetime(2021,1,1), catchup=True, default_args=default_args)
def weather_data_dag():

    @task()
    def extract_google_analytics_data():
        print("hello dag")
        return

   extract_google_analytics_data()

weather_data_dag = weather_data_dag()