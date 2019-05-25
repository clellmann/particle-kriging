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

config = Variable.get('validate_kriging_config', deserialize_json=True)

searching_grids = validation.grid_search(config['PARAMS'])

dag = DAG(dag_id='validate_kriging', 
    description='Kriging Validation', 
    default_args=default_args, 
    schedule_interval = None, 
    catchup=False
)

def execute_cross_validation(i, search_grid, **kwargs):
    """
    Deploys task for execution of cross validation.

    Args:
        i (int): Grid search run id.
        search_grid (dict): Current searched grid.
    """
    cross_val_tab = kwargs['ti']
    dag = kwargs['dag']
    task_instance = kwargs['task_instance']
    for j, fold in enumerate(cross_val_tab['fold'].unique()):
        train = cross_val_tab[cross_val_tab['fold'] != fold]
        test = cross_val_tab[cross_val_tab['fold'] == fold]

        distance_matrix = PythonOperator(
            task_id='distance_matrix'+str(i)+str(j),
            python_callable=semivariogram.calculate_dist_matrix(base_df=train),
            dag=dag,
        )

        variogram_cloud = PythonOperator(
            task_id='variogram_cloud'+str(i)+str(j),
            python_callable=task_helper.wrap_xcom_task(task_function=semivariogram.calc_variogram_cloud, xcom_name='dist_df', prev_task_id='distance_matrix'+str(i)+str(j), max_range=search_grid['MAX_RANGE']),
            dag=dag,
        )

        empirical_variogram = PythonOperator(
            task_id='empirical_variogram'+str(i)+str(j),
            python_callable=task_helper.wrap_xcom_task(task_function=semivariogram.calc_variogram_df, xcom_name='semivar_df', prev_task_id='variogram_cloud'+str(i)+str(j), distance_bins=search_grid['DISTANCE_BINS']),
            dag=dag,
        )

        semivariogram = PythonOperator(
            task_id='semivariogram'+str(i)+str(j),
            python_callable=task_helper.wrap_xcom_task(task_function=semivariogram.fit_semivariograms_pm, xcom_name='variogram_df', prev_task_id='empirical_variogram'+str(i)+str(j), max_range=search_grid['MAX_RANGE']),
            dag=dag,
        )

        grid  = PythonOperator(
            task_id='grid'+str(i)+str(j),
            python_callable=grid.get_grid_from_test_points(test_df=test),
            dag=dag,
        )

        kriging = PythonOperator(
            task_id='kriging'+str(i)+str(j),
            python_callable=task_helper.wrap_triple_xcom_task(task_function=kriging.ordinary_kriging_pm, xcom_names=['grid', 'distance_df', 'semivariograms'], prev_task_ids=['grid'+str(i)+str(j), 'distance_matrix'+str(i)+str(j), 'semivariogram'+str(i)+str(j)], train_df=train, max_range=search_grid['MAX_RANGE']),
            dag=dag,
        )

        statistics = PythonOperator(
            task_id='statistics'+str(i)+str(j),
            python_callable=task_helper.wrap_xcom_task(task_function=validation.calc_validation_statistic, xcom_name='result_df', prev_task_id='kriging', test_df=test),
            dag=dag,
        )

        result = PythonOperator(
            task_id='result'+str(i)+str(j),
            python_callable=task_helper.print_xcom_task(prev_task_id='statistics'),
            dag=dag,
        )

        task_instance >> distance_matrix >> variogram_cloud >> empirical_variogram >> semivariogram
        task_instance >> [grid, get_raw_data, distance_matrix, semivariogram] >> kriging >> statistics >> result


for i, search_grid in enumerate(searching_grids):
    if config['EXECUTION_MODE'] == 'live-data':
        get_raw_data = PythonOperator(
            task_id='get_live_raw_data'+str(i),
            python_callable=data_load.get_raw_data(search_grid['BOUNDING_BOX']),
            dag=dag,
        )
    if config['EXECUTION_MODE'] == 'db-data':
        get_raw_data = PythonOperator(
            task_id='get_db_raw_data'+str(i),
            python_callable=data_load.get_raw_db_data(search_grid['BOUNDING_BOX'], search_grid['TIMESTAMP']),
            dag=dag,
        )

    cross_validate_split = PythonOperator(
        task_id='cross_validate_split'+str(i),
        python_callable=task_helper.wrap_xcom_task(task_function=validation.cross_validation_split, xcom_name='raw_data', prev_task_id='get_raw_data', folds=config['CROSS_VALIDATION_FOLDS']),
        dag=dag,
    )

    cross_validation = PythonOperator(
        task_id='cross_validate_split'+str(i),
        python_callable=execute_cross_validation(i, search_grid),
        dag=dag,
    )

    get_raw_data >> cross_validate_split >> cross_validation

    
