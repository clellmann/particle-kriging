import sys
sys.path.insert(0, "functions")
sys.path.insert(0, "utils")
from functions.data_load import *
from functions.distance_matrix import *
from functions.grid import *
from functions.kriging import *
from functions.semivariogram import *
from functions.validation import *
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

config = Variable.get('validate_kriging_config', deserialize_json=True)

searching_grids = grid_search(config['PARAMS_LIVE']) if config['EXECUTION_MODE'] == 'live-data' else grid_search(config['PARAMS_DB'])

dag = DAG(dag_id='validate_kriging', 
    description='Kriging Validation', 
    default_args=default_args, 
    schedule_interval = None, 
    catchup=False
)

def execute_cross_validation(i, search_grid, prev_task_id, **kwargs):
    """
    Deploys task for execution of cross validation.

    Args:
        i (int): Grid search run id.
        search_grid (dict): Current searched grid.
        prev_task_id (str): Id of the previous task.
    """
    cross_val_tab = kwargs['ti'].xcom_pull(task_ids=prev_task_id)
    statistics = []
    for j, fold in enumerate(cross_val_tab['fold'].unique()):
        train = cross_val_tab[cross_val_tab['fold'] != fold]
        test = cross_val_tab[cross_val_tab['fold'] == fold]

        distance_matrix = calculate_dist_matrix(base_df=train)
        variogram_cloud = calc_variogram_cloud(dist_df=distance_matrix, max_range=search_grid['MAX_RANGE'])
        empirical_variogram = calc_variogram_df(semivar_df=variogram_cloud, distance_bins=search_grid['DISTANCE_BINS'])
        semivariogram = fit_semivariograms_pm(variogram_df=empirical_variogram, max_range=search_grid['MAX_RANGE'])
        grid = get_grid_from_test_points(test_df=test)
        kriging = ordinary_kriging_pm(grid=grid, distance_df=distance_matrix, semivariograms=semivariogram, train_df=train, max_range=search_grid['MAX_RANGE'])
        statistics.append(calc_validation_statistic(result_df=kriging, test_df=test))
    return calc_validation_statistic_overall(statistics=statistics)


for i, search_grid in enumerate(searching_grids):
    if config['EXECUTION_MODE'] == 'live-data':
        get_raw_data = PythonOperator(
            task_id='get_raw_data'+str(i),
            python_callable=get_raw_data,
            op_kwargs={'bounding_box': search_grid['BOUNDING_BOX']},
            dag=dag,
        )
    if config['EXECUTION_MODE'] == 'db-data':
        get_raw_data = PythonOperator(
            task_id='get_raw_data'+str(i),
            python_callable=get_raw_db_data,
            op_kwargs={'bounding_box': search_grid['BOUNDING_BOX'], 'timestamp': search_grid['TIMESTAMP']},
            dag=dag,
        )

    cross_validate_split = PythonOperator(
        task_id='cross_validate_split'+str(i),
        python_callable=wrap_xcom_task,
        op_kwargs={'task_function': cross_validation_split, 
                   'xcom_name': 'raw_data', 
                   'prev_task_id': 'get_raw_data'+str(i), 
                   'folds': config['CROSS_VALIDATION_FOLDS']},
        dag=dag,
    )

    cross_validation = PythonOperator(
        task_id='cross_validation'+str(i),
        python_callable=execute_cross_validation,
        op_kwargs={'i': i, 'search_grid': search_grid, 'prev_task_id': 'cross_validate_split'+str(i)},
        dag=dag,
    )

    result = PythonOperator(
        task_id='result'+str(i),
        python_callable=print_xcom_task,
        op_kwargs={'prev_task_id': 'cross_validation'+str(i)},
        dag=dag,
    )

    get_raw_data >> cross_validate_split >> cross_validation >> result

    
