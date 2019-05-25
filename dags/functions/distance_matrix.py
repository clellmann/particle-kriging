import pandas as pd


def calculate_dist_matrix(base_df, **kwargs):
    """
    Calculates the distance matrix df.

    Args:
      base_df (pandas.DataFrame): Data Frame containing the training data.

    Returns (pandas.DataFrame): Data Frame containing the distance matrix.
    """
    base_df['key'] = 1
    merged_df = pd.merge(base_df, base_df, on='key')
    dist_df = merged_df[['id_x', 'id_y']]
    dist_df['dist'] = merged_df.apply(lambda row: haversine((row['latitude_x'], row['longitude_x']), (row['latitude_y'], row['longitude_y']))*1000, axis = 1)
    dist_df['time_diff'] = merged_df.apply(lambda row: (row['timestamp_x'] - row['timestamp_y']).total_seconds(), axis=1)
    dist_df['P1_diff'] = merged_df['P1_x'] - merged_df['P1_y']
    dist_df['P2_diff'] = merged_df['P2_x'] - merged_df['P2_y']

    return dist_df
