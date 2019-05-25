import numpy as np
import pandas as pd
from sklearn.model_selection import KFold
from sklearn.model_selection import ParameterGrid

def cross_validation_split(raw_data, folds, **kwargs):
    """
    Splits a raw data PM data frame in cross validation folds.

    Args:
        raw_data (pandas.DataFrame): Raw data data frame of PM data.
        folds (int): Number of folds.

    Returns (pandas.DataFrame): Raw data data frame with added folds column
    """
    kf = KFold(n_splits = folds, shuffle = True, random_state = 2)
    for i, group in enumerate(kf.split(raw_data)):
        raw_data.loc[group[1], 'fold'] = i
    raw_data['fold'] = raw_data['fold'].astype('int')

    return raw_data


def grid_search(params):
    """
    Creates the grid search parameters out of params to search.

    Args:
        params (dict): Dict of array of parameters.

    Returns
    """
    return list(ParameterGrid(params))


def calc_validation_statistic(result_df, test_df, **kwargs):
    """
    Calculates the validation statistics.

    Args:
        result_df (pandas.DataFrame): Result data frame with kriged PM data.
        test_df (pandas.DataFrame): Data Frame with test PM data for kriged points.

    Returns (dict): Statistics dict.
    """
    joined_df = pd.merge(result_df, test_df, on=['fold', 'fold2'], suffixes=('_result', '_test'))
    rmse_p1 = np.sqrt(np.mean((joined_df['P1_result'] - joined_df['P1_test'])**2))
    rmse_p2 = np.sqrt(np.mean((joined_df['P2_result'] - joined_df['P2_test'])**2))

    return {'RMSE_P1': rmse_p1, 'RMSE_P2': rmse_p2}


def calc_validation_statistic_overall(statistics, **kwargs):
    """
    Calculates the validation overall statistics.

    Args:
        statistics (list): List of statistic dicts.

    Returns (dict): Overall statistics dict.
    """
    statistic_df = pd.DataFrame(statistics)

    return statistic_df.agg(['mean']).to_dict()
