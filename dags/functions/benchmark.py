import numpy as np
import pandas as pd

def calc_benchmark_mean_statistic(train_df, test_df, **kwargs):
    """
    Calculates the benchamrk statistics for mean prediction model.

    Args:
        train_df (pandas.DataFrame): Data Frame with training PM data for benchmark predictor.
        test_df (pandas.DataFrame): Data Frame with test PM data for prediction points.

    Returns (dict): Statistics dict.
    """
    p1_mean = train_df['P1'].mean()
    p2_mean = train_df['P2'].mean()
    joined_df = test_df
    joined_df['P1_mean'] = p1_mean
    joined_df['P2_mean'] = p2_mean
    rmse_p1 = np.sqrt(np.mean((joined_df['P1_mean'] - joined_df['P1'])**2))
    rmse_p2 = np.sqrt(np.mean((joined_df['P2_mean'] - joined_df['P2'])**2))

    return {'MEAN_P1': p1_mean, 'RMSE_P1': rmse_p1, 'MEAN_P2': p2_mean, 'RMSE_P2': rmse_p2}


def calc_benchmark_mean_statistic_overall(statistics, **kwargs):
    """
    Calculates the benchmark overall statistics.

    Args:
        statistics (list): List of statistic dicts.

    Returns (dict): Overall statistics dict.
    """
    statistic_df = pd.DataFrame(statistics)

    return statistic_df.agg(['mean']).to_dict()