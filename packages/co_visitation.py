import pandas as pd
from shapely.geometry import Point
from shapely import wkb
import shapely
import matplotlib.pyplot as plt

def preprocess_data(traffic, locations):
    """
    Preprocess the data: convert column labels and handle geometries.
    """
    traffic.columns = traffic.columns.str.lower()
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
    point = Point(row['longitude'], row['latitude'])
    return point.within(location.GEOMETRY)

def add_location_columns(traffic, locations):
    """
    Add a column with the location name for each signal record.
    """
    for location in locations.itertuples():
        traffic[location.LOCATION] = traffic.apply(is_in_location, axis=1, args=(location,))
    return traffic

def calculate_co_visitation(traffic, locations):
    """
    Calculate and return the co-visitation matrix.
    """
    if 'user_id' in traffic.columns:
        visit_counts = traffic.groupby('user_id').sum()
        visit_counts_numeric = visit_counts.select_dtypes(include='number')
        visit_counts_numeric = visit_counts_numeric.fillna(0)
        visit_counts['total_visits'] = visit_counts_numeric.sum(axis=1)
        co_visits = visit_counts[visit_counts['total_visits'] > 1]

        co_visit_matrix = pd.DataFrame(index=locations['LOCATION'], columns=locations['LOCATION'])

        for loc1 in locations['LOCATION']:
            for loc2 in locations['LOCATION']:
                if loc1 != loc2:
                    co_visit_matrix.loc[loc1, loc2] = co_visits[(co_visits[loc1] > 0) & (co_visits[loc2] > 0)].shape[0]
                else:
                    co_visit_matrix.loc[loc1, loc2] = co_visits[co_visits[loc1] > 0].shape[0]

        return co_visit_matrix.astype(float)  # Ensure all data is numeric
    else:
        print("The 'user_id' column does not exist in the 'traffic' data")

def create_matrix(traffic, locations, plot=False, output_file='co_visitation_matrix.jpg', title='Movements between given locations'):
    traffic, locations = preprocess_data(traffic, locations)
    traffic = add_location_columns(traffic, locations)
    co_visit_matrix = calculate_co_visitation(traffic, locations)

    if plot:
        # Plotting the co-visitation matrix
        plt.figure(figsize=(10, 8))
        plt.imshow(co_visit_matrix.values, cmap='Blues', interpolation='nearest')
        plt.title(title)
        plt.colorbar()
        plt.xticks(range(len(co_visit_matrix)), co_visit_matrix.columns, rotation=45)
        plt.yticks(range(len(co_visit_matrix)), co_visit_matrix.index)

        # Add text annotations for travel values in the center of each cell
        for i in range(len(co_visit_matrix)):
            for j in range(len(co_visit_matrix)):
                plt.text(j, i, f'{co_visit_matrix.iloc[i, j]:.0f}', ha='center', va='center', color='black')

        plt.tight_layout()

        # Save plot as a JPG image
        plt.savefig(output_file)
        plt.close()

        print(f"Co-visitation matrix saved as {output_file}")
        return co_visit_matrix
    else:
        print(co_visit_matrix)

if __name__ == "__main__":
    traffic_path = 'traffic.csv'
    traffic = pd.read_csv(traffic_path)
    locations_path = 'locations.parquet'
    locations = pd.read_parquet(locations_path)
    create_matrix(traffic, locations, plot=True)
