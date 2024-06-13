import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import matplotlib.pyplot as plt

def match_traffic_to_location(traffic, locations):
    """Match each traffic record to the nearest location."""
    # Convert traffic DataFrame to GeoDataFrame
    traffic_gdf = gpd.GeoDataFrame(
        traffic, geometry=gpd.points_from_xy(traffic['longitude'], traffic['latitude']), crs="EPSG:4326"
    )

    # Convert locations DataFrame to GeoDataFrame and set CRS
    locations_gdf = gpd.GeoDataFrame(locations, geometry=gpd.points_from_xy(locations['lng'], locations['lat']), crs="EPSG:4326")

    # Perform spatial join to find the nearest location
    traffic_with_locations = gpd.sjoin_nearest(traffic_gdf, locations_gdf, how='left', distance_col='distance')

    return traffic_with_locations

def calculate_hourly_structure(location, traffic):
    """Calculate the hourly structure of signals for a given location."""
    location_visits = traffic[traffic['location'] == location]
    location_visits['hour'] = location_visits['occured_at'].dt.hour
    hourly_distribution = (location_visits.groupby('hour').size() / len(location_visits)) * 100  # Multiply by 100 to get percentage
    return hourly_distribution

def plot_hourly_structures(hourly_structures, output_jpg):
    """Plot hourly structures for each location."""
    fig, ax = plt.subplots(len(hourly_structures), 1, figsize=(10, 20))

    for i, (loc, hourly_structure) in enumerate(hourly_structures.items()):
        ax[i].bar(hourly_structure.index, hourly_structure.values)
        ax[i].set_title(f'Hourly Traffic Structure - {loc}')
        ax[i].set_xlabel('Hour')
        ax[i].set_ylabel('Percentage of Signals')  # Keep the ylabel as Percentage of Signals

    plt.tight_layout()
    plt.savefig(output_jpg)  # Save to jpg file
    plt.close()  # Close the plot window

def print_hourly_structures(hourly_structures):
    """Print hourly structures for each location."""
    for loc, hourly_structure in hourly_structures.items():
        print(f"Hourly Traffic Structure - {loc}:")
        for hour, percentage in hourly_structure.items():
            print(f"Hour {hour}: {percentage:.2f}%")
        print()

def process_and_plot_traffic_data(traffic, locations, output_jpg='hourly_structures.jpg', plot=True):
    """Process traffic data and plot/print hourly structures for each location."""
    # Convert column names to lowercase
    traffic.columns = traffic.columns.str.lower()
    locations.columns = locations.columns.str.lower()

    # Print the columns to debug
    print("Traffic columns:", traffic.columns)
    print("Locations columns:", locations.columns)

    # Convert 'occured_at' to datetime
    traffic['occured_at'] = pd.to_datetime(traffic['occured_at'], errors='coerce')

    # Match traffic data to locations
    traffic_with_locations = match_traffic_to_location(traffic, locations)

    # Calculate hourly structures
    hourly_structures = {loc: calculate_hourly_structure(loc, traffic_with_locations) for loc in locations['location']}

    if plot:
        # Plot hourly structures and save to jpg
        plot_hourly_structures(hourly_structures, output_jpg)
    else:
        # Print hourly structures
        print_hourly_structures(hourly_structures)

if __name__ == "__main__":
    # Paths to data files
    traffic_csv_path = 'data/DATAPLACE_TRAFFIC.csv'
    locations_parquet_path = 'data/Dataplace_locations.parquet'

    # Load data
    traffic = pd.read_csv(traffic_csv_path)
    locations = pd.read_parquet(locations_parquet_path)

    process_and_plot_traffic_data(traffic, locations, plot=False)
