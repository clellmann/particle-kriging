import os
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
import pickle

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


def save_parameters(parameters, **kwargs):
    """
    Saves the grid search parameters of a run.

    Args:
        parameters (dict): Dict of grid search parameters.

    Returns (NoneType): None.
    """
    print(parameters)
    filename = "/usr/local/airflow/results/{0}/{1}.pkl".format(kwargs['dag_run'].run_id, kwargs['task'].task_id)
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename,"wb") as file:
        pickle.dump(parameters, file)
    return None


def get_test_data(fold, prev_task_id, **kwargs):
    """
    Gets the test data for given fold.

    Args:
        fold (int): Number of fold.
        prev_task_id (str): Id of the previous task.

    Returns (pandas.DataFrame): Test data frame.
    """
    cross_val_tab = kwargs['ti'].xcom_pull(task_ids=prev_task_id)
    result = cross_val_tab[cross_val_tab['fold'] == fold]
    filename = "/usr/local/airflow/results/{0}/{1}.pkl".format(kwargs['dag_run'].run_id, kwargs['task'].task_id)
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename,"wb") as file:
        pickle.dump(result, file)
    return result


def get_train_data(fold, prev_task_id, **kwargs):
    """
    Gets the train data for given fold.

    Args:
        fold (int): Number of fold.
        prev_task_id (str): Id of the previous task.

    Returns (pandas.DataFrame): Train data frame.
    """
    cross_val_tab = kwargs['ti'].xcom_pull(task_ids=prev_task_id)
    result = cross_val_tab[cross_val_tab['fold'] != fold]
    filename = "/usr/local/airflow/results/{0}/{1}.pkl".format(kwargs['dag_run'].run_id, kwargs['task'].task_id)
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename,"wb") as file:
        pickle.dump(result, file)
    return result


dag = DAG(dag_id='validate_kriging', 
    description='Kriging Validation', 
    default_args=default_args, 
    schedule_interval = None, 
    catchup=False
)

for i, search_grid in enumerate(searching_grids):
    get_parameters = PythonOperator(
        task_id='get_parameters'+str(i),
        python_callable=save_parameters,
        op_kwargs={'parameters': search_grid},
        dag=dag,
    )

    if config['EXECUTION_MODE'] == 'live-data':
        get_raw_data = PythonOperator(
            task_id='get_raw_data'+str(i),
            python_callable=wrap_simple_task,
            op_kwargs={'task_function': get_raw_data,
                    'bounding_box': search_grid['BOUNDING_BOX']},
            dag=dag,
        )
    if config['EXECUTION_MODE'] == 'db-data':
        get_raw_data = PythonOperator(
            task_id='get_raw_data'+str(i),
            python_callable=wrap_simple_task,
            op_kwargs={'task_function': get_raw_db_data,
                    'bounding_box': search_grid['BOUNDING_BOX'], 
                    'timestamp': search_grid['TIMESTAMP']},
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

    for j in range(config['CROSS_VALIDATION_FOLDS']):
        test = PythonOperator(
            task_id='test'+str(i)+str(j),
            python_callable=get_test_data,
            op_kwargs={'fold': j,
                    'prev_task_id': 'cross_validate_split'+str(i)},
            dag=dag,
        )

        train = PythonOperator(
            task_id='train'+str(i)+str(j),
            python_callable=get_train_data,
            op_kwargs={'fold': j,
                    'prev_task_id': 'cross_validate_split'+str(i)},
            dag=dag,
        )

        distance_matrix = PythonOperator(
            task_id='distance_matrix'+str(i)+str(j),
            python_callable=wrap_xcom_task,
            op_kwargs={'task_function': calculate_dist_matrix, 
                    'xcom_name': 'base_df', 
                    'prev_task_id': 'train'+str(i)+str(j)},
            dag=dag,
        )

        variogram_cloud = PythonOperator(
            task_id='variogram_cloud'+str(i)+str(j),
            python_callable=wrap_xcom_task,
            op_kwargs={'task_function': calc_variogram_cloud, 
                    'xcom_name': 'dist_df', 
                    'prev_task_id': 'distance_matrix'+str(i)+str(j),
                    'max_range': search_grid['MAX_RANGE']},
            dag=dag,
        )

        empirical_variogram = PythonOperator(
            task_id='empirical_variogram'+str(i)+str(j),
            python_callable=wrap_xcom_task,
            op_kwargs={'task_function': calc_variogram_df, 
                    'xcom_name': 'semivar_df', 
                    'prev_task_id': 'variogram_cloud'+str(i)+str(j), 
                    'distance_bins': search_grid['DISTANCE_BINS']},
            dag=dag,
        )

        semivariogram = PythonOperator(
            task_id='semivariogram'+str(i)+str(j),
            python_callable=wrap_xcom_task,
            op_kwargs={'task_function': fit_semivariograms_pm, 
                    'xcom_name': 'variogram_df', 
                    'prev_task_id': 'empirical_variogram'+str(i)+str(j), 
                    'max_range': search_grid['MAX_RANGE']},
            dag=dag,
        )

        grid = PythonOperator(
            task_id='grid'+str(i)+str(j),
            python_callable=wrap_xcom_task,
            op_kwargs={'task_function': get_grid_from_test_points, 
                    'xcom_name': 'test_df', 
                    'prev_task_id': 'test'+str(i)+str(j)},
            dag=dag,
        )

        kriging = PythonOperator(
            task_id='kriging'+str(i)+str(j),
            python_callable=wrap_quadruple_xcom_task,
            op_kwargs={'task_function': ordinary_kriging_pm, 
                    'xcom_names': ['grid', 'distance_df', 'semivariograms', 'train_df'], 
                    'prev_task_ids': ['grid'+str(i)+str(j), 'distance_matrix'+str(i)+str(j), 'semivariogram'+str(i)+str(j), 'train'+str(i)+str(j)], 
                    'max_range': search_grid['MAX_RANGE']},
            dag=dag,
        )

        statistics = PythonOperator(
            task_id='statistics'+str(i)+str(j),
            python_callable=wrap_double_xcom_task,
            op_kwargs={'task_function': calc_validation_statistic, 
                    'xcom_names': ['result_df', 'test_df'], 
                    'prev_task_ids': ['kriging'+str(i)+str(j), 'test'+str(i)+str(j)]},
            dag=dag,
        )

        result = PythonOperator(
            task_id='result'+str(i)+str(j),
            python_callable=print_xcom_task,
            op_kwargs={'prev_task_id': 'statistics'+str(i)+str(j)},
            dag=dag,
        )

        get_parameters >> get_raw_data >> cross_validate_split >> [train, test]
        train >> distance_matrix >> variogram_cloud >> empirical_variogram >> semivariogram
        test >> grid
        [train, grid, distance_matrix, semivariogram] >> kriging
        [kriging, test] >> statistics >> result
