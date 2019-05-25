from haversine import haversine
import math

def calculate_spatial_point_in_distance_and_bearing(start_point, distance, bearing):
    """
    Calculates a destination in distance from given location in bearing direction.
    
    Args:
        start_point (tuple): Spatial start point in degree in lat/lon order.
        distance (float): Distance to destination in metres.
        bearing (float): Bearing to destination point in degree.
        
    Returns (tuple): Destination point in lat/lon order.
    """
    R = 6378.1 #Radius of the Earth in km
    brng = math.radians(bearing) #Bearing converted to radians.
    d = distance/1000 #Distance in km
    start_lat = start_point[0]
    start_lon = start_point[1]

    lat1 = math.radians(start_lat) #Current lat point converted to radians
    lon1 = math.radians(start_lon) #Current long point converted to radians

    lat2 = math.asin( math.sin(lat1)*math.cos(d/R) +
         math.cos(lat1)*math.sin(d/R)*math.cos(brng))

    lon2 = lon1 + math.atan2(math.sin(brng)*math.sin(d/R)*math.cos(lat1),
                 math.cos(d/R)-math.sin(lat1)*math.sin(lat2))

    lat2 = math.degrees(lat2)
    lon2 = math.degrees(lon2)

    return (round(lat2, 5), round(lon2, 5))


def calculate_spatial_grid(bounding_box, distance, **kwargs):
    """
    Calculates an equidistant spatial grid in given bounding box.
    
    Args:
        bounding_box (list): Bounding box given as list of NW and SE tuples in lat/lon order.
        distance (float): Distance between grid points in metres.
        
    Returns (list): List of grid points in lat/lon order.
    """
    current_column_point = bounding_box[0]
    current_row_point = bounding_box[0]
    grid = []
    for i in range(int(haversine(bounding_box[0], (bounding_box[0][0], bounding_box[1][1]))*1000/distance)):
        grid.append(current_column_point)
        current_row_point = current_column_point
        for j in range(int(haversine(bounding_box[0], (bounding_box[1][0], bounding_box[0][1]))*1000/distance)):
            current_row_point = calculate_spatial_point_in_distance_and_bearing(current_row_point, distance, 180)
            grid.append(current_row_point)
        current_column_point = calculate_spatial_point_in_distance_and_bearing(current_column_point, distance, 90)
    return grid


def get_grid_from_test_points(test_df, **kwargs):
    """
    Extracts the grid points from test points.

    Args:
        test_df: Data frame with test data and spatial points.

    Returns (list): List of grid points in lat/lon order.
    """
    grid = []
    for index, row in test_df.iterrows():
        grid.append((row['latitude'], row['longitude']))
    return grid
