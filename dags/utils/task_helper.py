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
    

def wrap_double_xcom_task(task_function, prev_task_ids, **kwargs):
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

    return task_function(**{**kwargs, **{xcom_name[0]: xcom0, xcom_name[1]: xcom1}})
