
import json
import pickle
import io
from datetime import timedelta, datetime

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.http.operators.http import HttpHook
from airflow.macros import ds_add, ds_format
from ncdc.ncdc_helpers import get_average_temp, get_city_ids, get_location_string

from google.cloud import bigquery

import pandas as pd


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

cities = [
    {"country": "United States", "name": "New York", "state":"New York"},
    {"country": "United States", "name": "Los Angeles", "state":"California"},
    {"country": "United States", "name": "Chicago", "state":"Illinois"},
    {"country": "United States", "name": "Houston", "state":"Texas"},
    {"country": "United States", "name": "Phoenix", "state":"Arizona"},
    {"country": "United States", "name": "Philadelphia", "state":"Pennsylvania"},
    {"country": "United States", "name": "San Antonio", "state":"Texas"},
    {"country": "United States", "name": "San Diego", "state":"California"},
    {"country": "United States", "name": "Dallas", "state":"Texas"},
    {"country": "United States", "name": "San Jose", "state":"California"},
    {"country": "United States", "name": "Austin", "state":"Texas"},
    {"country": "United States", "name": "Jacksonville", "state":"Florida"},
    {"country": "United States", "name": "Fort Worth", "state":"Texas"},
    {"country": "United States", "name": "Columbus", "state":"Ohio"},
    {"country": "United States", "name": "Charlotte", "state":"North Carolina"},
    {"country": "United States", "name": "San Francisco", "state":"California"},
    {"country": "United States", "name": "Indianapolis", "state":"Indiana"},
    {"country": "United States", "name": "Seattle", "state":"Washington"},
    {"country": "United States", "name": "Denver", "state":"Colorado"},
    {"country": "United States", "name": "Washington", "state":"District of Columbia"},
    {"country": "United States", "name": "Boston", "state":"Massachusetts"},
    {"country": "United States", "name": "El Paso", "state":"Texas"},
    {"country": "United States", "name": "Nashville", "state":"Tennessee"},
    {"country": "United States", "name": "Detroit", "state":"Michigan"},
    {"country": "United States", "name": "Oklahoma City", "state":"Oklahoma"},
    {"country": "United States", "name": "Portland", "state":"Oregon"},
    {"country": "United States", "name": "Las Vegas", "state":"Nevada"},
    {"country": "United States", "name": "Memphis", "state":"Tennessee"},
    {"country": "United States", "name": "Louisville", "state":"Kentucky"},
    {"country": "United States", "name": "Baltimore", "state":"Maryland"},
    {"country": "United States", "name": "Milwaukee", "state":"Wisconsin"},
    {"country": "United States", "name": "Albuquerque", "state":"New Mexico"},
    {"country": "United States", "name": "Tucson", "state":"Arizona"},
    {"country": "United States", "name": "Fresno", "state":"California"},
    {"country": "United States", "name": "Mesa", "state":"Arizona"}
]

@dag(schedule_interval="@daily", start_date=datetime(2016,8,1), end_date=datetime(2016,8,2), catchup=True, default_args=default_args)
def weather_data_dag():

    @task()
    def extract_google_analytics_data(gcp_conn_id:str,start_date:str, end_date:str, locations:str):
        bq_hook = BigQueryHook(gcp_conn_id=gcp_conn_id)

        cities = ','.join(['"{}"'.format(c["name"]) for c in locations])
        countries = ','.join(['"{}"'.format(c["country"]) for c in locations])
        states = ','.join(['"{}"'.format(c["state"]) for c in locations])
        
        query = """SELECT 
                        fullVisitorId,
                        date,
                        geoNetwork.city as city,
                        geoNetwork.country as country,
                        geoNetwork.region as region,
                        product.v2ProductCategory as productCategory,
                        product.v2ProductName as productName,
                        hits.eCommerceAction.action_type as action_type,
                        hits.eCommerceAction.step as action_step,
                        product.productQuantity as quantity,
                        product.productPrice as price,
                        product.productRevenue as revenue,
                        product.isImpression as isImpression,
                        hits.transaction.transactionId as transactionId,
                        hits.transaction.transactionRevenue as transactionRevenue,
                        hits.transaction.transactionTax as transactionTax,
                        hits.transaction.transactionShipping as transactionShipping,
                    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
                        UNNEST(hits) as hits,
                        UNNEST(hits.product) as product
                    WHERE 
                        _TABLE_SUFFIX BETWEEN '{start_date}' AND '{end_date}'
                    AND
                        geoNetwork.country IN ({countries})
                    AND
                        geoNetwork.city IN ({cities})
                    AND 
                        geoNetwork.region IN ({states})
                    ORDER BY 
                        date ASC""".format(start_date=start_date, end_date=end_date, countries=countries, cities=cities, states=states)
        
        df = bq_hook.get_pandas_df(sql = query, dialect="standard")

        df["location_name"] = df.apply(lambda row : get_location_string(row['country'], row['city'], row['region']), axis = 1) 
        df["date"] = pd.to_datetime(df["date"])
        df["price"]=df["price"]/(10**6)
        df["revenue"]=df["revenue"]/(10**6)
        df["transactionRevenue"]=df["transactionRevenue"]/(10**6)
        df["transactionTax"]=df["transactionTax"]/(10**6)
        df["transactionShipping"]=df["transactionShipping"]/(10**6)
        
        df = df.set_index(["date", "location_name"])
        return df.to_csv(date_format="%Y-%m-%d %H:%M:%S")

    @task()
    def extract_weather_data(http_conn_id:str,dataset_id:str, start_date:str, end_date:str, cities:list, datatype_id:str, units:str):
        http_hook = HttpHook(
            method='GET',
            http_conn_id=http_conn_id,
        )
        city_ids = get_city_ids(http_hook, cities)
        weather_data = get_average_temp(http_hook=http_hook, dataset_id=dataset_id, location_ids=city_ids, start_date=start_date, end_date=end_date, datatype_id=datatype_id,units=units)
        
        weather_data = weather_data.reset_index()
        weather_data["date"] = pd.to_datetime(weather_data["date"])

        return weather_data.to_csv(date_format="%Y-%m-%d %H:%M:%S", index=False)
        
    @task()
    def transform(weather_data, analytics_data):
        weather_df = pd.read_csv(io.StringIO(weather_data))        
        analytics_df = pd.read_csv(io.StringIO(analytics_data))
        
        combined_df = analytics_df.merge(weather_df,how="inner", right_on=["date", "location_name"], left_on=["date", "location_name"])
        combined_df = combined_df.replace(to_replace="", value=None)
        combined_df = combined_df.set_index(["date", "location_name"])

        return combined_df.to_csv(date_format="%Y-%m-%d %H:%M:%S")

    @task()
    def load(gcp_conn_id:str, combined_data:str, gcs_bucket:str, gcs_object:str):
        gcs_hook = GCSHook(
            gcp_conn_id=gcp_conn_id
        )
        gcs_hook.upload(bucket_name=gcs_bucket, data=combined_data, object_name=gcs_object)
        bq_hook = BigQueryHook(bigquery_conn_id=gcp_conn_id)
        bq_hook.run_load(destination_project_dataset_table="augmented-works-297410.demo_dataset.sales_interactions2",
                source_uris="gs://{}/{}".format(gcs_bucket, gcs_object),
                write_disposition="WRITE_APPEND",
                source_format="CSV",
                skip_leading_rows=1,
                autodetect = False,
                schema_fields=[
                        bigquery.SchemaField("date", "DATETIME").to_api_repr(),
                        bigquery.SchemaField("location_name", "STRING").to_api_repr(),
                        bigquery.SchemaField("average_temp", "FLOAT").to_api_repr(),
                        bigquery.SchemaField("fullVisitorId", "STRING").to_api_repr(),
                        bigquery.SchemaField("city", "STRING").to_api_repr(),
                        bigquery.SchemaField("country", "STRING").to_api_repr(),
                        bigquery.SchemaField("region", "STRING").to_api_repr(),
                        bigquery.SchemaField("productCategory", "STRING").to_api_repr(),
                        bigquery.SchemaField("productName", "STRING").to_api_repr(),
                        bigquery.SchemaField("action_type", "INTEGER").to_api_repr(),
                        bigquery.SchemaField("action_step", "INTEGER").to_api_repr(),
                        bigquery.SchemaField("quantity", "FLOAT").to_api_repr(),
                        bigquery.SchemaField("price", "FLOAT").to_api_repr(),
                        bigquery.SchemaField("revenue", "FLOAT").to_api_repr(),
                        bigquery.SchemaField("isImpression", "BOOL").to_api_repr(),
                        bigquery.SchemaField("transactionId", "STRING").to_api_repr(),
                        bigquery.SchemaField("transactionRevenue", "FLOAT").to_api_repr(),
                        bigquery.SchemaField("transactionTax", "FLOAT").to_api_repr(),
                        bigquery.SchemaField("transactionShipping", "FLOAT").to_api_repr(),
                    ]
                )
    
    end_date = '{{ macros.ds_format(macros.ds_add( next_ds, -1), "%Y-%m-%d", "%Y%m%d") }}'
    start_date = '{{ ds_nodash }}'
   
    analytics_data = extract_google_analytics_data(gcp_conn_id="google_cloud_default", start_date=start_date, end_date=end_date, locations = cities)
    weather_data  = extract_weather_data(http_conn_id="ncdc_default", dataset_id="GHCND", start_date=start_date, end_date=end_date, cities=cities, datatype_id="TAVG", units="metric")
    
    combined_data = transform(analytics_data, weather_data)
    
    load(gcp_conn_id="google_cloud_default", combined_data=combined_data, gcs_bucket="demo-bucket.airlaunch.ch", gcs_object="{{ dag.dag_id }}/{{ task.task_id }}/{{ ds }}.csv")

weather_data_dag = weather_data_dag()