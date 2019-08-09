import sys
sys.path.insert(0, "functions")
sys.path.insert(0, "utils")
from functions.data_load import *
from functions.distance_matrix import *
from functions.grid import *
from functions.kriging import *
from functions.semivariogram import *
from utils.task_helper import *
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

default_args = {
    'owner' : 'kriging',
    'depends_on_past': False,
    'start_date': datetime.now() - timedelta(days=1),
    'provide_context': True,
    'email': ['kriging@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

config = Variable.get('live_kriging_config', deserialize_json=True)

dag = DAG(dag_id='live_kriging', 
    description='Scheduled kriging of live PM data', 
    default_args=default_args, 
    schedule_interval = '*/30 * * * *', 
    catchup=False
)

get_raw_data = PythonOperator(
    task_id='get_raw_data',
    python_callable=wrap_simple_task,
    op_kwargs={'task_function': get_raw_live_data,
               'bounding_box': config['BOUNDING_BOX']},
    dag=dag,
)

distance_matrix = PythonOperator(
    task_id='distance_matrix',
    python_callable=wrap_xcom_task,
    op_kwargs={'task_function': calculate_dist_matrix, 
               'xcom_names': ['base_df'], 
               'prev_task_ids': ['get_raw_data']},
    dag=dag,
)

variogram_cloud = PythonOperator(
    task_id='variogram_cloud',
    python_callable=wrap_xcom_task,
    op_kwargs={'task_function': calc_variogram_cloud, 
               'xcom_names': ['dist_df'], 
               'prev_task_ids': ['distance_matrix'],
               'max_range': config['MAX_RANGE']},
    dag=dag,
)

empirical_variogram = PythonOperator(
    task_id='empirical_variogram',
    python_callable=wrap_xcom_task,
    op_kwargs={'task_function': calc_variogram_df, 
               'xcom_names': ['semivar_df'], 
               'prev_task_ids': ['variogram_cloud'], 
               'distance_bins': config['DISTANCE_BINS']},
    dag=dag,
)

semivariogram = PythonOperator(
    task_id='semivariogram',
    python_callable=wrap_xcom_task,
    op_kwargs={'task_function': fit_semivariograms_pm, 
               'xcom_names': ['variogram_df'], 
               'prev_task_ids': ['empirical_variogram'], 
               'max_range': config['MAX_RANGE']},
    dag=dag,
)

grid  = PythonOperator(
    task_id='grid',
    python_callable=wrap_simple_task,
    op_kwargs={'task_function': calculate_spatial_grid, 
               'bounding_box': config['BOUNDING_BOX'], 
               'distance': config['TARGET_GRID']},
    dag=dag,
)

kriging = PythonOperator(
    task_id='kriging',
    python_callable=wrap_xcom_task,
    op_kwargs={'task_function': ordinary_kriging_pm, 
               'xcom_names': ['grid', 'distance_df', 'semivariograms', 'train_df'], 
               'prev_task_ids': ['grid', 'distance_matrix', 'semivariogram', 'get_raw_data'], 
               'max_range': config['MAX_RANGE']},
    dag=dag,
)

result = PythonOperator(
    task_id='result',
    python_callable=print_xcom_task,
    op_kwargs={'prev_task_ids': ['kriging']},
    dag=dag,
)

get_raw_data >> distance_matrix >> variogram_cloud >> empirical_variogram >> semivariogram
[grid, get_raw_data, distance_matrix, semivariogram] >> kriging >> result
