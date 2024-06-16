import pandas as pd
import pygeos
import datetime as dt

def load_data():
    # Load user traffic data
    traffic = pd.read_csv('traffic.csv')

    # Load locations data
    locations = pd.read_parquet('locations.parquet')

    return traffic, locations

def normalize_column_names(df):
    df.columns = map(str.lower, df.columns)
    return df

def find_users_between_locations(traffic, locations, location1, location2):
    traffic = normalize_column_names(traffic)
    locations = normalize_column_names(locations)

    try:
        location1_geometry_bytes = locations.loc[locations['location'] == location1, 'geometry'].values[0]
        location2_geometry_bytes = locations.loc[locations['location'] == location2, 'geometry'].values[0]
    except IndexError:
        print(f"Location '{location1}' or '{location2}' not found in the locations dataset.")
        return None

    location1_geometry = pygeos.io.from_wkb(location1_geometry_bytes)
    location2_geometry = pygeos.io.from_wkb(location2_geometry_bytes)

    def is_within_location(row, geometry):
        user_geometry = pygeos.points(row['longitude'], row['latitude'])
        return pygeos.intersects(user_geometry, geometry)

    users_in_location1 = traffic[traffic.apply(is_within_location, axis=1, geometry=location1_geometry)]
    users_in_location2 = traffic[traffic.apply(is_within_location, axis=1, geometry=location2_geometry)]

    commuting_users = pd.merge(users_in_location1, users_in_location2, on='user_id')
    return commuting_users


def filter_time_signals(traffic, later_than=22, earlier_than=5):
    traffic = normalize_column_names(traffic)

    # Convert timestamp to datetime
    traffic['occured_at'] = pd.to_datetime(traffic['occured_at'])

    # Filter signals between 22:00 and 05:00
    night_signals = traffic[(traffic['occured_at'].dt.hour >= later_than) | (traffic['occured_at'].dt.hour < earlier_than)]
    return night_signals

def estimate_user_locations(travel_data, locations_data, location_name):
    travel_data = normalize_column_names(travel_data)
    locations_data = normalize_column_names(locations_data)

    # Retrieve binary geometry for the specified location
    location_geometry_bytes = locations_data.loc[locations_data['location'] == location_name, 'geometry'].values[0]
    location_geometry = pygeos.io.from_wkb(location_geometry_bytes)

    def is_within_location(row, geometry):
        user_geometry = pygeos.points(row['longitude'], row['latitude'])
        return pygeos.intersects(user_geometry, geometry)

    # Filter travel data to include only users in the specified location
    users_in_location = travel_data[travel_data.apply(is_within_location, axis=1, geometry=location_geometry)]

    # Calculate mean location of users in the specified location
    home_locations = users_in_location.groupby('user_id').agg({
        'latitude': 'mean',
        'longitude': 'mean'
    }).reset_index()

    return home_locations

def analyze_user_movements(traffic, locations, location1, location2):
    # Find users who were in both locations
    commuting_users = find_users_between_locations(traffic, locations, location1, location2)
    return commuting_users

def analyze_travel_and_users(traffic, locations, location1, location2, later_than=22, earlier_than=5):
    # Find users traveling between location1 and location2
    traveling_users = find_users_between_locations(traffic, locations, location1, location2)

    if traveling_users is None or traveling_users.empty:
        print(f"No users found traveling between {location1} and {location2}.")
        return {'travel': None, 'users': None, 'estimated_locations': None}

    # Filter for time signals between location1 and location2 during specified hours
    time_filtered_traffic = filter_time_signals(traffic, later_than, earlier_than)
    time_filtered_users = time_filtered_traffic[time_filtered_traffic['user_id'].isin(traveling_users['user_id'])]

    # Estimate user locations for users in location1
    estimated_locations = estimate_user_locations(time_filtered_users, locations, location2)

    return {'travel': time_filtered_traffic, 'users': time_filtered_users, 'estimated_locations': estimated_locations}


if __name__ == "__main__":
    traffic, locations = load_data()

    # Specify the locations and time filters for analysis
    work_location = "Mordor na Domaniewskiej"
    commute_location = "Arkadia"
    home_location = "Osiedle Wilanów"
    #home_location_mordor = "Mordor na Domaniewskiej"

    # Filter for work hours signals between Mordor na Domaniewskiej and Arkadia (7:00-15:00)
    work_hours_users = analyze_user_movements(traffic, locations, work_location, commute_location)
    work_hours_traffic = filter_time_signals(traffic, 7, 15)
    work_hours_users = work_hours_traffic[work_hours_traffic['user_id'].isin(work_hours_users['user_id'])]

    # Estimate user locations for users in Mordor na Domaniewskiej
    estimated_location_work = estimate_user_locations(work_hours_users, locations, work_location)

    print(f"\nEstimated Home Locations for users commuting between {home_location} and {commute_location}:")
    print(estimated_location_work)

    # Filter for night signals between Mordor na Domaniewskiej and Arkadia (22:00-5:00)
    night_users = analyze_user_movements(traffic, locations, home_location, commute_location)
    night_traffic = filter_time_signals(traffic, 22, 5)  # Assumes night crossing midnight
    night_users = night_traffic[night_traffic['user_id'].isin(night_users['user_id'])]

    # Estimate home locations for users in Mordor na Domaniewskiej
    estimated_location_mordor = estimate_user_locations(night_users, locations, home_location)

    print(f"\nEstimated Home Locations for users commuting between {home_location} and {commute_location} at night:")
    print(estimated_location_mordor)
