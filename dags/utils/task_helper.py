def wrap_xcom_task(task_function, xcom_name, prev_task_id, **kwargs):
    """
    Wraps a function for a task with xcom.

    Args:
        task_function (function): Function to wrap.
        xcom_name (str): Name of the xcom parameter for task_function.
        prev_task_id (str): Id of the previous task.
        **kwargs: Function arguments.

    Returns: Function returns as xcom.
    """
    ti = kwargs.pop('ti')

    xcom = ti.xcom_pull(task_ids=prev_task_id)

    return task_function(**{**kwargs, **{xcom_name: xcom}})
    

def wrap_double_xcom_task(task_function, xcom_names, prev_task_ids, **kwargs):
    """
    Wraps a function for a task with two xcoms.

    Args:
        task_function (function): Function to wrap.
        xcom_names (list): Names of the xcom parameter for task_function.
        prev_task_id (list): Ids of the previous tasks.
        **kwargs: Function arguments.

    Returns: Function returns as xcom.
    """
    ti = kwargs.pop('ti')

    xcom0, xcom1 = ti.xcom_pull(task_ids=prev_task_ids)

    return task_function(**{**kwargs, **{xcom_names[0]: xcom0, xcom_names[1]: xcom1}})


def wrap_triple_xcom_task(task_function, xcom_names, prev_task_ids, **kwargs):
    """
    Wraps a function for a task with three xcoms.

    Args:
        task_function (function): Function to wrap.
        xcom_names (list): Names of the xcom parameter for task_function.
        prev_task_id (list): Ids of the previous tasks.
        **kwargs: Function arguments.

    Returns: Function returns as xcom.
    """
    ti = kwargs.pop('ti')

    xcom0, xcom1, xcom2 = ti.xcom_pull(task_ids=prev_task_ids)

    return task_function(**{**kwargs, **{xcom_names[0]: xcom0, xcom_names[1]: xcom1, xcom_names[2]: xcom2}})


def wrap_quadruple_xcom_task(task_function, xcom_names, prev_task_ids, **kwargs):
    """
    Wraps a function for a task with four xcoms.

    Args:
        task_function (function): Function to wrap.
        xcom_names (list): Names of the xcom parameter for task_function.
        prev_task_id (list): Ids of the previous tasks.
        **kwargs: Function arguments.

    Returns: Function returns as xcom.
    """
    ti = kwargs.pop('ti')

    xcom0, xcom1, xcom2, xcom3 = ti.xcom_pull(task_ids=prev_task_ids)

    return task_function(**{**kwargs, **{xcom_names[0]: xcom0, xcom_names[1]: xcom1, xcom_names[2]: xcom2, xcom_names[3]: xcom3}})


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
