from pipeline_utils import extract, transform, load
import config
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# import pendulum
# import logging

# ---------- Direct calling without Airflow (for transitioning and debugging phases)
raw_flights_data = extract(destinations=config.destination_countries)
clean_df, avg_df, airlines_df = transform(raw_flights_data)
load(clean_df,'clean_flights_data.csv')
load(airlines_df,'airlines_data.csv')
load(avg_df,'flights_avg.csv')

# ----------- Airflow DAG
# dag = DAG(
#     dag_id='flights_pipeline',
#     default_args={
#         'start_date': pendulum.datetime(2024, 3, 17, tz='UTC'),
#         'max_tries': 2,
#         'schedule_interval': '@daily'
#     }
# )
#
#
# def extract_data(**kwargs):
#     """ Similar to 'extract' from pipeline_utils.py - returns the raw data frame,
#         but also makes it usable in following Airflow tasks (like transform)"""
#     raw_flights_data = extract(destinations=config.destination_countries)
#     logging.info("Data extraction completed.")
#     return raw_flights_data
#
#
# def transform_data(**kwargs):
#     """ Similar to 'transform' from pipeline_utils.py - incorporates airflow XComs to
#     push cleaned data frames and makes them usable for final load task"""
#     ti = kwargs['ti']
#     raw_flights_data = ti.xcom_pull(task_ids='extract_data')
#     clean_df, avg_df, airlines_df = transform(raw_flights_data)
#     ti.xcom_push(key='clean_df', value=clean_df)
#     ti.xcom_push(key='avg_df', value=avg_df)
#     ti.xcom_push(key='airlines_df', value=airlines_df)
#     logging.info("Data transformation completed.")
#
#
# def load_data(**kwargs):
#     """ Similar to 'load' from pipeline_utils.py - exports the cleaned data frames
#     to .csv files """
#     ti = kwargs['ti']
#     clean_df = ti.xcom_pull(key='clean_df', task_ids='transform_data')
#     avg_df = ti.xcom_pull(key='avg_df', task_ids='transform_data')
#     airlines_df = ti.xcom_pull(key='airlines_df', task_ids='transform_data')
#     load(clean_df, 'clean_flights_data.csv')
#     load(avg_df, 'flights_avg.csv')
#     load(airlines_df, 'airlines_data.csv')
#     logging.info("Data load completed.")
#
#
# extract_operator = PythonOperator(
#     task_id='extract_data',
#     python_callable=extract_data,
#     dag=dag
# )
#
# transform_operator = PythonOperator(
#     task_id='transform_data',
#     python_callable=transform_data,
#     provide_context=True,
#     dag=dag
# )
#
# load_operator = PythonOperator(
#     task_id='load_data',
#     python_callable=load_data,
#     provide_context=True,
#     dag=dag
# )
#
# extract_operator >> transform_operator >> load_operator
