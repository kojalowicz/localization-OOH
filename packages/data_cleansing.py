import pandas as pd
import geopandas as gpd
from shapely import wkb
import shapely

def load_data(data_path):
    """Load data from CSV or Parquet based on file extension."""
    if data_path.endswith('.csv'):
        return pd.read_csv(data_path)
    elif data_path.endswith('.parquet'):
        return gpd.read_parquet(data_path)
    else:
        raise ValueError("Unsupported file format. Only CSV and Parquet are supported.")

def save_data(data, output_path):
    """Save data to CSV or Parquet based on file extension."""
    if output_path.endswith('.csv'):
        data.to_csv(output_path, index=False)
    elif output_path.endswith('.parquet'):
        data.to_parquet(output_path, index=False)
    else:
        raise ValueError("Unsupported file format. Only CSV and Parquet are supported.")
    print(f"Cleaned data saved to {output_path}.")

def remove_duplicates(data):
    """Remove duplicate rows from the DataFrame."""
    duplicates = data.duplicated()
    if duplicates.any():
        print(f"Number of duplicate rows: {duplicates.sum()}")
        data = data.drop_duplicates()
        print("Duplicates removed.")
    else:
        print("No duplicate rows found.")
    return data

def remove_missing_values(data, key_columns):
    """Remove rows with missing values in specified key columns."""
    data = data.dropna(subset=key_columns)
    print("Rows with missing values removed.")
    return data

def convert_to_datetime(data, column):
    """Convert a column to datetime format and remove invalid dates."""
    data[column] = pd.to_datetime(data[column], errors='coerce')
    data = data.dropna(subset=[column])
    print("Invalid dates removed.")
    return data

def validate_coordinates(data, lat_col='LAT', lng_col='LNG'):
    """Remove rows with invalid latitude/longitude values."""
    data = data[(data[lng_col] >= -180) & (data[lng_col] <= 180) &
                (data[lat_col] >= -90) & (data[lat_col] <= 90)]
    print("Rows with invalid latitude/longitude values removed.")
    return data

def remove_zero_coordinates(data, lat_col='LAT', lng_col='LNG'):
    """Remove rows with zero or null latitude/longitude values."""
    # Debug: print columns to check which are available
    print(f"Available columns: {data.columns}")

    data = data[(data[lng_col] != 0) & (data[lat_col] != 0) &
                (~data[lng_col].isnull()) & (~data[lat_col].isnull())]
    print("Rows with zero or null latitude/longitude values removed.")
    return data

def wkb_to_geometry(geometry):
    """Convert WKB or Shapely geometry to Shapely geometry."""
    if isinstance(geometry, bytes):
        try:
            return wkb.loads(geometry, hex=True)
        except shapely.errors.GEOSException:
            return None
    elif isinstance(geometry, shapely.geometry.base.BaseGeometry):
        return geometry
    elif isinstance(geometry, str):
        try:
            return shapely.wkt.loads(geometry)
        except shapely.errors.GEOSException:
            return None
    else:
        raise TypeError(f"Expected bytes, Shapely geometry, or str, got {type(geometry).__name__}")

def clean_traffic_data(traffic_input):
    """Load, clean, and save traffic data."""
    traffic = remove_duplicates(traffic_input)
    key_columns = ['USER_ID', 'OCCURED_AT', 'LONGITUDE', 'LATITUDE']
    traffic = remove_missing_values(traffic, key_columns)
    traffic = convert_to_datetime(traffic, 'OCCURED_AT')
    traffic = validate_coordinates(traffic, 'LATITUDE', 'LONGITUDE')
    traffic = remove_zero_coordinates(traffic, 'LATITUDE', 'LONGITUDE')
    return traffic

def clean_bud_data(bud_input):
    """Load, clean, and save BUD data."""

    bud = remove_duplicates(bud_input)

    key_columns = ['LOKALNYID', 'FUNOGOLNABUDYNKU_DESC', 'LICZBAKONDYGNACJI', 'AREA', 'GEOMETRY']
    bud = remove_missing_values(bud, key_columns)

    # Convert LICZBAKONDYGNACJI and AREA to numeric values
    bud.loc[:, 'LICZBAKONDYGNACJI'] = pd.to_numeric(bud['LICZBAKONDYGNACJI'], errors='coerce')
    bud.loc[:, 'AREA'] = pd.to_numeric(bud['AREA'], errors='coerce')

    # Remove rows with invalid LICZBAKONDYGNACJI or AREA
    bud = bud.dropna(subset=['LICZBAKONDYGNACJI', 'AREA'])
    print("Invalid LICZBAKONDYGNACJI or AREA values removed.")

    # Apply wkb_to_geometry function to GEOMETRY column
    bud.loc[:, 'GEOMETRY'] = bud['GEOMETRY'].apply(wkb_to_geometry)
    print("Invalid GEOMETRY values removed.")

    return bud

def clean_population_data(population_input):
    """Load, clean, and save population data."""

    population = remove_duplicates(population_input)

    key_columns = [
        'FEMALE', 'MALE', 'FEMALE0003', 'FEMALE0307', 'FEMALE0812', 'FEMALE1315', 'FEMALE1618',
        'FEMALE1924', 'FEMALE2529', 'FEMALE3034', 'FEMALE3539', 'FEMALE4044', 'FEMALE4549',
        'FEMALE5054', 'FEMALE5559', 'FEMALE6064', 'FEMALE6569', 'FEMALE7074', 'FEMALE7579',
        'FEMALE8084', 'FEMALE8589', 'FEMALE9099', 'MALE0003', 'MALE0307', 'MALE0812', 'MALE1315',
        'MALE1618', 'MALE1924', 'MALE2529', 'MALE3034', 'MALE3539', 'MALE4044', 'MALE4549', 'MALE5054',
        'MALE5559', 'MALE6064', 'MALE6569', 'MALE7074', 'MALE7579', 'MALE8084', 'MALE8589', 'MALE9099',
        'LAT', 'LNG', 'TOTAL'
    ]

    population = remove_missing_values(population, key_columns)
    population = validate_coordinates(population)

    # Convert population columns to numeric values
    population[key_columns[:-3]] = population[key_columns[:-3]].apply(pd.to_numeric, errors='coerce')

    # Remove rows with invalid population values
    population = population.dropna(subset=key_columns[:-3])
    print("Invalid population values removed.")


    return population

def clean_locations(locations_input):
    """Load, clean, and save locations data."""
    locations = remove_duplicates(locations_input)
    key_columns = ["location", "lat", "lng", "geometry"]
    locations = remove_missing_values(locations, key_columns)
    locations = remove_zero_coordinates(locations, 'lat', 'lng')
    locations = validate_coordinates(locations, 'lat', 'lng')
    #locations['geometry'] = locations['geometry'].apply(wkb_to_geometry)

    return locations

if __name__ == "__main__":
    # Paths to the CSV and Parquet files
    traffic_csv_path = 'TRAFFIC.csv'
    traffic_output_path = 'TRAFFIC_CLEANED.csv'

    bud_csv_path = 'BUDB.csv'
    bud_output_path = 'BUDB_CLEANED.csv'

    population_csv_path = 'POPULATION.csv'
    population_output_path = 'POPULATION_CLEANED.csv'

    locations_parquet_path = 'locations.parquet'
    locations_output_path = 'locations_cleaned.parquet'

    # Clean the traffic data
    traffic = load_data(traffic_csv_path)
    traffic_cleaned = clean_traffic_data(traffic)
    save_data(traffic_cleaned, traffic_output_path)

    # Clean the BUD data
    bud = load_data(bud_csv_path)
    bud_cleaned = clean_bud_data(bud)
    save_data(bud_cleaned, bud_output_path)

    # Clean the population data
    population = load_data(population_csv_path)
    population_cleaned = clean_population_data(population)
    save_data(population_cleaned, population_output_path)

    # Clean the locations data
    locations = load_data(locations_parquet_path)
    locations_cleaned = clean_locations(locations)
    save_data(locations_cleaned, locations_output_path)
