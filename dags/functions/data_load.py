import pandas as pd
import requests
from datetime import datetime

def transform_pm_data(response_items, bounding_box):
    """
    Transforms pm data to the target format with time, location and pm values.
    
    Args:
        response_items (list): List of pm data from luftdaten.info.
        bounding_box (list): Bounding box as lat/lon tuples NW, SE.
        
    Returns (list): Transformed data.
    """
    pd_items = []
    for item in response_items:
        if (bounding_box[0][1] < float(item['location']['longitude']) < bounding_box[1][1]) and (bounding_box[1][0] < float(item['location']['latitude']) < bounding_box[0][0]):
            base_dict = {'latitude': float(item['location']['latitude']),
                         'longitude': float(item['location']['longitude']),
                         'altitude': float(item['location']['altitude']), 
                         'sensor_id': item['sensor']['id'], 
                         'timestamp': datetime.strptime(item['timestamp'], "%Y-%m-%d %H:%M:%S")}
            for sensorvalue in item['sensordatavalues']:
                if sensorvalue['value_type'] == 'P1' or sensorvalue['value_type'] == 'P2':
                    base_dict[sensorvalue['value_type']] = float(sensorvalue['value'])
            pd_items.append(base_dict)
    return pd_items


def filter_anomalous_pm(pm_data):
    """
    Filters out anomalous pm data:
    
    Args:
        pm_data (list): List of pm data.
        
    Returns (list): Filtered data.
    """
    return [data for data in pm_data if (data.get('P1') is not None and data.get('P1') < 1000) and (data.get('P2') is not None and data.get('P2') < 1000)]


def get_raw_data(bounding_box):
    """
    Downloads the current raw data.

    Args:
        bounding_box (list): Bounding box as lat/lon tuples NW, SE.
    
    Returns (pandas.DataFrame): Data Frame containing the raw PM data.
    """
    API = 'http://api.luftdaten.info/static/v1/data.json'


    def filter_pm_data(json):
        """
        Filters only sensors with PM data.
        Args:
            json (list): List of sensor data jsons.
        Returns (list): List of filtered sensor data jsons.
        """
        return [elem for elem in json if any([True if 'P1' in data.values() or 'P2' in data.values() else False for data in elem['sensordatavalues']]) and elem['location']['altitude']]

    res = requests.get(API).json()
    response_items = filter_pm_data(res)
    base_df = pd.DataFrame(filter_anomalous_pm(transform_pm_data(response_items, bounding_box)))
    base_df['id'] = base_df.index
    return base_df
