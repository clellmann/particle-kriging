import numpy as np
import pandas as pd
from haversine import haversine

def krige_point(distance_vector, distance_matrix, semivariogram, train_values):
    """
    Kriges a point (kriging based location prediction).
    
    Args:
        distance_vector (np.array): Distance vector from training points to prediction point. Shape: (1,number of training points)
        distance_matrix (np.matrix): Distance_matrix of training points. Shape: (number of training points,number of training points)
        semivariogram (Semivarogram): Fitted semivariogram.
        train_values (np.array): Particulate matter densities of training set. Shape: (1,number of training points)
        
    Returns (tuple): Tuple with predicted particulate matter density and standard deviation.
    """
    semivar_func = np.vectorize(semivariogram.exponential, [float])
    γ0 = semivar_func(distance_vector)
    γ0_transpose = np.transpose(γ0)
    Γ = semivar_func(distance_matrix)
    Γ_inv = np.linalg.pinv(Γ)
    ones = np.ones((len(distance_vector), 1))
    ones_transpose = np.transpose(ones)
    
    particulate_matter = np.asscalar(np.transpose(γ0 - ones*((ones_transpose*Γ_inv*γ0-1)/(ones_transpose*Γ_inv*ones)))*Γ_inv*train_values)
    std = np.asscalar(γ0_transpose*Γ_inv*γ0-((ones_transpose*Γ_inv*γ0-1)**2/(ones_transpose*Γ_inv*ones)))
    
    return (particulate_matter, std)


def ordinary_kriging_pm(grid, distance_df, semivariograms, train_df, max_range, **kwargs):
    """
    Calculates the ordinary kriging predicates for given grid.
    
    Args:
        grid (list): List of spatial points to krige in lat/lon tuples.
        distance_df (pandas.DataFrame): Distance data frame.
        semivariograms (dict): Dict of fitted semivariograms for PM data.
        train_df (pandas.DataFrame): Trainings data data frame.
        max_range (float): Maximum range for taking training data into account.
        
    Returns (pandas.DataFrame): Data frame with grid prediction result relations.
    """
    rows = []
    for point in grid:
        result = {}
        points_in_range = train_df
        points_in_range['distances'] = points_in_range.apply(lambda row: haversine((row['latitude'], row['longitude']), point)*1000, axis = 1)
        points_in_range = points_in_range[points_in_range['distances'] <= max_range]
        distance_vector = points_in_range.sort_values(by=['id'])['distances'].values.reshape((points_in_range.sort_values(by=['id'])['distances'].values.shape[0], 1))
        distance_df_in_range = distance_df[np.isin(distance_df['id_x'], points_in_range['id'].values) & np.isin(distance_df['id_y'], points_in_range['id'].values)]
        distance_matrix = np.matrix(np.split(distance_df_in_range.sort_values(by=['id_x', 'id_y'])['dist'].values, len(points_in_range)))
        for pm in semivariograms.keys():
            train_values = points_in_range.sort_values(by=['id'])[pm].values.reshape((points_in_range.sort_values(by=['id'])[pm].values.shape[0], 1))
            particulate_matter, std = krige_point(distance_vector, distance_matrix, semivariograms[pm], train_values)
            result.update({pm: particulate_matter, 'std_'+pm: std}) 
        rows.append({**{'latitude': point[0], 'longitude': point[1]},**result})
        
    return pd.DataFrame(rows)