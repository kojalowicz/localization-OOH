import pandas as pd
from shapely.geometry import Point
from shapely import wkb
import shapely

def load_data(traffic_path, locations_path):
    """
    Load traffic and locations data.

    Parameters:
    traffic_path (str): Path to the traffic CSV file.
    locations_path (str): Path to the locations Parquet file.

    Returns:
    DataFrame, DataFrame: DataFrames containing traffic and locations data.
    """
    traffic = pd.read_csv(traffic_path)
    locations = pd.read_parquet(locations_path)
    return traffic, locations

def preprocess_data(traffic, locations):
    """
    Preprocess the data: convert column labels and handle geometries.
    """
    traffic.columns = traffic.columns.str.upper()
    traffic['OCCURED_AT'] = pd.to_datetime(traffic['OCCURED_AT'])
    locations.columns = locations.columns.str.upper()

    locations = locations.dropna(subset=['GEOMETRY'])

    def wkb_to_geometry(wkb_bytes):
        try:
            return wkb.loads(wkb_bytes, hex=True)
        except shapely.errors.GEOSException:
            return None

    locations['GEOMETRY'] = locations['GEOMETRY'].apply(wkb_to_geometry)
    locations = locations.dropna(subset=['GEOMETRY'])

    return traffic, locations

def is_in_location(row, location):
    """
    Check if a user is in a specified location.
    """
    point = Point(row['LONGITUDE'], row['LATITUDE'])
    return point.within(location.GEOMETRY)

def add_location_columns(traffic, locations):
    """
    Add a column with the location name for each signal record.
    """
    for location in locations.itertuples():
        traffic[location.LOCATION] = traffic.apply(is_in_location, axis=1, args=(location,))
    return traffic

def get_repeat_visits_df(traffic, locations):
    """
    Get a DataFrame of repeat visits for each location.

    Parameters:
    traffic (DataFrame): DataFrame containing traffic data.
    locations (DataFrame): DataFrame containing location data with geometry.

    Returns:
    DataFrame, dict: DataFrame containing repeat visit details, and dictionary with location names as keys and repeat visit counts as values.
    """
    repeat_visits_details = []
    repeat_visits_summary = {}

    for location in locations.itertuples():
        location_traffic = traffic[traffic[location.LOCATION]].copy()
        user_visit_counts = location_traffic.groupby('USER_ID').size()
        repeat_users = user_visit_counts[user_visit_counts > 1].index
        repeat_visits = location_traffic[location_traffic['USER_ID'].isin(repeat_users)].copy()

        # Add a visit count column to track the number of visits by each user
        repeat_visits['VISIT_COUNT'] = repeat_visits.groupby('USER_ID').cumcount() + 1

        # Filter only the repeated visits
        repeat_visits = repeat_visits[repeat_visits['VISIT_COUNT'] > 1]

        # Calculate the time difference between visits
        repeat_visits['PREVIOUS_VISIT'] = repeat_visits.groupby('USER_ID')['OCCURED_AT'].shift(1)
        repeat_visits['TIME_DIFF_DAYS'] = (repeat_visits['OCCURED_AT'] - repeat_visits['PREVIOUS_VISIT']).dt.days

        # Filter out visits with a time difference greater than 14 days
        repeat_visits = repeat_visits[repeat_visits['TIME_DIFF_DAYS'] <= 14]

        # Add the location name to the repeat visits DataFrame
        repeat_visits['LOCATION'] = location.LOCATION

        repeat_visits_details.append(repeat_visits)
        repeat_visits_summary[location.LOCATION] = repeat_visits['USER_ID'].nunique()

    repeat_visits_df = pd.concat(repeat_visits_details)
    return repeat_visits_df, repeat_visits_summary

def calculate_visit_frequencies(repeat_visits_df):
    """
    Calculate the visit frequencies for each location and return a summary DataFrame.
    """
    visit_frequency_summary = []

    grouped = repeat_visits_df.groupby(['LOCATION', 'USER_ID']).size().reset_index(name='VISIT_COUNT')
    for location, group in grouped.groupby('LOCATION'):
        total_users = group['USER_ID'].nunique()
        visit_counts = group['VISIT_COUNT'].value_counts().sort_index()

        visit_frequency_percentages = (visit_counts / total_users * 100).reset_index()
        visit_frequency_percentages.columns = ['VISIT_COUNT', 'PERCENTAGE']
        visit_frequency_percentages['LOCATION'] = location

        visit_frequency_summary.append(visit_frequency_percentages)

    visit_frequency_summary_df = pd.concat(visit_frequency_summary, ignore_index=True)
    return visit_frequency_summary_df

def calculate_and_print_repeat_frequencies(traffic, locations):
    """
    Main function to get details of repeat visits for all locations.

    """

    # Preprocess data
    traffic, locations = preprocess_data(traffic, locations)

    # Add location columns to traffic data
    traffic = add_location_columns(traffic, locations)

    # Get details of repeat visits for each location
    repeat_visits_df, repeat_visits_summary = get_repeat_visits_df(traffic, locations)

    # Print the details of repeat visits
    print("Repeat visits summary:", repeat_visits_summary)

    # Calculate visit frequencies
    if not repeat_visits_df.empty:
        visit_frequency_summary_df = calculate_visit_frequencies(repeat_visits_df)
        print(visit_frequency_summary_df)
    else:
        print("No visit frequencies to summarize.")

if __name__ == "__main__":
    # Define the path to the traffic CSV file
    traffic_path = "TRAFFIC.csv"

    # Define the path to the locations Parquet file
    locations_path = 'locations.parquet'

    traffic, locations = load_data(traffic_path, locations_path)

    calculate_and_print_repeat_frequencies(traffic, locations)
