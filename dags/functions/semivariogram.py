import numpy as np
import pandas as pd
from scipy.optimize import curve_fit

class Semivariogram():
    sill = 0
    range = 0
    nugget = 0
    
    def __init__(self):
        pass

    def spherical(self, h):
        if h == 0:
            y = 0
        if 0 < h <= self.range:
            y = self.nugget + (self.sill-self.nugget)*(3/2*h/self.range-1/2*(h/self.range)**3)
        if h > self.range:
            y = self.sill
        return y
    
    def spherical_fit(self, h, sill, nugget, range):
        self.sill = sill
        self.nugget = nugget
        self.range = range
        y = self.nugget + (self.sill-self.nugget)*(3/2*h/self.range-1/2*(h/self.range)**3)
        return y
    
    def exponential(self, h):
        if h == 0:
            y = 0
        if 0 < h:
            y = self.nugget + (self.sill-self.nugget)*(1-np.exp(-h/self.range))
        return y
    
    def exponential_fit(self, h, sill, nugget, range):
        self.sill = sill
        self.nugget = nugget
        self.range = range
        y = self.nugget + (self.sill-self.nugget)*(1-np.exp(-h/self.range))
        return y


def fit_semivariograms_pm(variogram_df, max_range):
    """
    Fits semivariograms to particulate matter data P1 and P2.

    Args:
       variogram_df (pandas.DataFrame): Data Frame containing the semivariances of bins.
       max_range (float): Maximum range for taking training data into account.

    Returns (dict): Dict containing the fitted semivariograms with PM key. 
    """
    semivariogram_exp_p1 = Semivariogram()
    semivariogram_exp_p2 = Semivariogram()
    semivario_df = variogram_df[np.array(variogram_df.index.values) <= max_range]
    curve_fit(semivariogram_exp_p1.exponential_fit, np.array(semivario_df.index.values), np.array(semivario_df['P1_semivar']['mean'].values), p0=[semivario_df['P1_semivar']['mean'].max(), semivario_df['P1_semivar']['mean'][0], semivario_df['P1_semivar']['mean'].idxmax()], bounds=(0,np.inf))
    curve_fit(semivariogram_exp_p2.exponential_fit, np.array(semivario_df.index.values), np.array(semivario_df['P2_semivar']['mean'].values), p0=[semivario_df['P2_semivar']['mean'].max(), semivario_df['P2_semivar']['mean'][0], semivario_df['P2_semivar']['mean'].idxmax()], bounds=(0,np.inf))
    return {'P1': semivariogram_exp_p1, 'P2': semivariogram_exp_p2}

    
def calc_variogram_df(semivar_df, distance_bins):
    """
    Calculates the empirical variogram df.

    Args:
      semivar_df (pandas.DataFrame): Data Frame containing the variogram cloud.
      distance_bins (float): Distance bin size in m.

      Returns (pandas.DataFrame): Data Frame containing the semivariances of bins.
    """    
    variogram_dist_df = semivar_df[['dist', 'P1_semivar', 'P2_semivar']]
    n_bins = np.ceil(variogram_dist_df['dist'].max()/distance_bins)
    variogram_dist_df['dist [m]'] = pd.cut(variogram_dist_df['dist'], n_bins, labels=np.arange(0, n_bins*distance_bins, distance_bins))

    def calc_errors(pm_semivars):
        """
        Calculates the errors of the semivariance averageing.
        
        Args:
            pm_semivars (list): List of pm_semivars (pandas df column).
            
        Returns (float): Central limit theorem error.
        """
        return np.std(pm_semivars)/np.sqrt(len(pm_semivars))

    return variogram_dist_df.drop(['dist'], axis=1).groupby('dist [m]').agg(['mean', calc_errors])


def calc_variogram_cloud(dist_df, max_range):
    """
    Calculates the variogram cloud df.

    Args:
      dist_df (pandas.DataFrame): Data Frame containing the distance matrix.
      max_range (float): Maximum range for taking training data into account.

    Returns (pandas.DataFrame):Data Frame containing the variogram cloud.
    """    
    semivar_df = dist_df[['id_x', 'id_y', 'dist', 'time_diff']].query('id_x < id_y').query('dist < {0}'.format(max_range))
    semivar_df['P1_semivar'] = np.square(dist_df['P1_diff'])/2
    semivar_df['P2_semivar'] = np.square(dist_df['P2_diff'])/2

    return semivar_df
