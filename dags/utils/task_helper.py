import pickle


def wrap_simple_task(task_function, **kwargs):
    """
    Wraps a function for a task without xcom and writes result to filesystem.

    Args:
        task_function (function): Function to wrap.
        **kwargs: Function arguments.

    Returns: Function returns as xcom.
    """
    result = task_function(**kwargs)
    with open("/usr/local/airflow/results/{0}/{1}.pkl".format(kwargs['dag_run'].run_id, task_function.__name__),"wb") as file:
        pickle.dump(result, file)
    return result


def wrap_xcom_task(task_function, xcom_name, prev_task_id, **kwargs):
    """
    Wraps a function for a task with xcom and writes result to filesystem.

    Args:
        task_function (function): Function to wrap.
        xcom_name (str): Name of the xcom parameter for task_function.
        prev_task_id (str): Id of the previous task.
        **kwargs: Function arguments.

    Returns: Function returns as xcom.
    """
    ti = kwargs.pop('ti')

    xcom = ti.xcom_pull(task_ids=prev_task_id)

    result = task_function(**{**kwargs, **{xcom_name: xcom}})
    with open("/usr/local/airflow/results/{0}/{1}.pkl".format(kwargs['dag_run'].run_id, task_function.__name__),"wb") as file:
        pickle.dump(result, file)
    return result
    

def wrap_double_xcom_task(task_function, xcom_names, prev_task_ids, **kwargs):
    """
    Wraps a function for a task with two xcoms and writes result to filesystem.

    Args:
        task_function (function): Function to wrap.
        xcom_names (list): Names of the xcom parameter for task_function.
        prev_task_id (list): Ids of the previous tasks.
        **kwargs: Function arguments.

    Returns: Function returns as xcom.
    """
    ti = kwargs.pop('ti')

    xcom0, xcom1 = ti.xcom_pull(task_ids=prev_task_ids)

    result = task_function(**{**kwargs, **{xcom_names[0]: xcom0, xcom_names[1]: xcom1}})
    with open("/usr/local/airflow/results/{0}/{1}.pkl".format(kwargs['dag_run'].run_id, task_function.__name__),"wb") as file:
        pickle.dump(result, file)
    return result


def wrap_triple_xcom_task(task_function, xcom_names, prev_task_ids, **kwargs):
    """
    Wraps a function for a task with three xcoms and writes result to filesystem.

    Args:
        task_function (function): Function to wrap.
        xcom_names (list): Names of the xcom parameter for task_function.
        prev_task_id (list): Ids of the previous tasks.
        **kwargs: Function arguments.

    Returns: Function returns as xcom.
    """
    ti = kwargs.pop('ti')

    xcom0, xcom1, xcom2 = ti.xcom_pull(task_ids=prev_task_ids)

    result = task_function(**{**kwargs, **{xcom_names[0]: xcom0, xcom_names[1]: xcom1, xcom_names[2]: xcom2}})
    with open("/usr/local/airflow/results/{0}/{1}.pkl".format(kwargs['dag_run'].run_id, task_function.__name__),"wb") as file:
        pickle.dump(result, file)
    return result


def wrap_quadruple_xcom_task(task_function, xcom_names, prev_task_ids, **kwargs):
    """
    Wraps a function for a task with four xcoms and writes result to filesystem.

    Args:
        task_function (function): Function to wrap.
        xcom_names (list): Names of the xcom parameter for task_function.
        prev_task_id (list): Ids of the previous tasks.
        **kwargs: Function arguments.

    Returns: Function returns as xcom.
    """
    ti = kwargs.pop('ti')

    xcom0, xcom1, xcom2, xcom3 = ti.xcom_pull(task_ids=prev_task_ids)

    result = task_function(**{**kwargs, **{xcom_names[0]: xcom0, xcom_names[1]: xcom1, xcom_names[2]: xcom2, xcom_names[3]: xcom3}})
    with open("/usr/local/airflow/results/{0}/{1}.pkl".format(kwargs['dag_run'].run_id, task_function.__name__),"wb") as file:
        pickle.dump(result, file)
    return result


def print_xcom_task(prev_task_id, **kwargs):
    """
    Prints xcom from a previous task.

    Args:
        prev_task_id (str): Id of the previous task.
        **kwargs: Environment.
    """
    ti = kwargs.pop('ti')

    xcom = ti.xcom_pull(task_ids=prev_task_id)

    print(xcom)
