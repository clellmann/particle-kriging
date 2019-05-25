import sys
sys.path.insert(0, "functions")
sys.path.insert(0, "utils")
from functions import *
from utils import *
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
    python_callable=data_load.get_raw_data(config['BOUNDING_BOX']),
    dag=dag,
)

distance_matrix = PythonOperator(
    task_id='distance_matrix',
    python_callable=task_helper.wrap_xcom_task(task_function=semivariogram.calculate_dist_matrix, xcom_name='base_df', prev_task_id='get_raw_data'),
    dag=dag,
)

variogram_cloud = PythonOperator(
    task_id='variogram_cloud',
    python_callable=task_helper.wrap_xcom_task(task_function=semivariogram.calc_variogram_cloud, xcom_name='dist_df', prev_task_id='distance_matrix', max_range=config['MAX_RANGE']),
    dag=dag,
)

empirical_variogram = PythonOperator(
    task_id='empirical_variogram',
    python_callable=task_helper.wrap_xcom_task(task_function=semivariogram.calc_variogram_df, xcom_name='semivar_df', prev_task_id='variogram_cloud', distance_bins=config['DISTANCE_BINS']),
    dag=dag,
)

semivariogram = PythonOperator(
    task_id='semivariogram',
    python_callable=task_helper.wrap_xcom_task(task_function=semivariogram.fit_semivariograms_pm, xcom_name='variogram_df', prev_task_id='empirical_variogram', max_range=config['MAX_RANGE']),
    dag=dag,
)

grid  = PythonOperator(
    task_id='grid',
    python_callable=grid.calculate_spatial_grid(config['BOUNDING_BOX'], config['TARGET_GRID']),
    dag=dag,
)

kriging = PythonOperator(
    task_id='kriging',
    python_callable=task_helper.wrap_quadruple_xcom_task(task_function=kriging.ordinary_kriging_pm, xcom_names=['grid', 'distance_df', 'semivariograms', 'train_df'], prev_task_ids=['grid', 'distance_matrix', 'semivariogram', 'get_raw_data'], max_range=config['MAX_RANGE']),
    dag=dag,
)

result = PythonOperator(
    task_id='result',
    python_callable=task_helper.print_xcom_task(prev_task_id='kriging'),
    dag=dag,
)

get_raw_data >> distance_matrix >> variogram_cloud >> empirical_variogram >> semivariogram
[grid, get_raw_data, distance_matrix, semivariogram] >> kriging >> result
