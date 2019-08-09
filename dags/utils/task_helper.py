import pickle
import os


def wrap_simple_task(task_function, **kwargs):
    """
    Wraps a function for a task without xcom and writes result to filesystem.

    Args:
        task_function (function): Function to wrap.
        **kwargs: Function arguments.

    Returns: Function returns as xcom.
    """
    result = task_function(**kwargs)
    filename = "/usr/local/airflow/results/{0}/{1}.pkl".format(kwargs['dag_run'].run_id, kwargs['task'].task_id)
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename,"wb") as file:
        pickle.dump(result, file)
    return result


def wrap_xcom_task(task_function, xcom_names, prev_task_ids, **kwargs):
    """
    Wraps a function for a task with xcoms and writes result to filesystem.

    Args:
        task_function (function): Function to wrap.
        xcom_names (list): Names of the xcom parameter for task_function.
        prev_task_id (list): Ids of the previous tasks.
        **kwargs: Function arguments.

    Returns: Function returns as xcom.
    """
    ti = kwargs.pop('ti')

    xcoms = {}
    for i, prev_task_id in enumerate(prev_task_ids):
        xcom = ti.xcom_pull(task_ids=prev_task_id)
        xcoms = {**xcoms, **{xcom_names[i]: xcom}}

    result = task_function(**{**kwargs, **xcoms})
    filename = "/usr/local/airflow/results/{0}/{1}.pkl".format(kwargs['dag_run'].run_id, kwargs['task'].task_id)
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename,"wb") as file:
        pickle.dump(result, file)
    return result


def wrap_xcom_list_task(task_function, prev_task_ids, **kwargs):
    """
    Wraps a function for a task with xcoms passed as values list and writes result to filesystem.

    Args:
        task_function (function): Function to wrap.
        prev_task_id (list): Ids of the previous tasks.
        **kwargs: Function arguments.

    Returns: Function returns as xcom.
    """
    ti = kwargs.pop('ti')

    xcoms = []
    for prev_task_id in prev_task_ids:
        xcom = ti.xcom_pull(task_ids=prev_task_id)
        xcoms.append(xcom)

    result = task_function(xcoms)
    filename = "/usr/local/airflow/results/{0}/{1}.pkl".format(kwargs['dag_run'].run_id, kwargs['task'].task_id)
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename,"wb") as file:
        pickle.dump(result, file)
    return result


def print_xcom_task(prev_task_ids, **kwargs):
    """
    Prints xcom from previous tasks with names.

    Args:
        prev_task_ids (str): Id of the previous task.
        **kwargs: Environment.
    """
    ti = kwargs.pop('ti')

    for prev_task_id in prev_task_ids:
        xcom = ti.xcom_pull(task_ids=prev_task_id)
        print("{0}: {1}".format(prev_task_id, str(xcom)))
