import pandas as pd
import argparse
import configparser
from shapely import wkb
from packages import (snowflake_data_handler, snowflake_csv_saver, co_visitation, data_cleansing, repeatability,
                      traffic_structure, demographic_structure, buildings_characteristics)


def load_config(config_file='config.ini'):
    config = configparser.ConfigParser()
    config.read(config_file)

    # Extract paths from the config and return as a dictionary
    paths_input = {key: value for key, value in config['paths_input'].items()}
    paths_output = {key: value for key, value in config['paths_output'].items()}
    jpg_output = {key: value for key, value in config['jpg_output'].items()}
    analysis = {key: value for key, value in config['analysis'].items()}

    return {"paths_input": paths_input, "paths_output": paths_output, "jpg_output": jpg_output, "analysis": analysis}

def load_data(paths):
    """
    Load data from CSV and Parquet files. Return empty DataFrames if files are missing.

    Parameters:
    paths (dict): Dictionary with file names as keys and file paths as values.

    Returns:
    dict: Dictionary with file names as keys and DataFrames as values.
    """
    data_frames = {}

    for key, path in paths.items():
        if path.endswith('.csv'):
            try:
                data_frames[key] = pd.read_csv(path)
            except FileNotFoundError:
                data_frames[key] = pd.DataFrame()
        elif path.endswith('.parquet'):
            try:
                data_frames[key] = pd.read_parquet(path)
            except FileNotFoundError:
                data_frames[key] = pd.DataFrame()
        else:
            data_frames[key] = pd.DataFrame()  # Handle unknown file types gracefully

    return data_frames

def download_csv(data_access):
    snowflake_csv_saver.download_csv_files(data_access)

def get_data_frames_from_snowflake(data_access):
    return snowflake_data_handler.sql_to_dataframes(data_access)

def main():
    parser = argparse.ArgumentParser(description="Data Analyzer")
    parser.add_argument("-d", "--download", action="store_true", help="Download CSV files")
    parser.add_argument("-c", "--connection", action="store_true", help="Use database connection")
    parser.add_argument("-p", "--plot", action="store_true", help="Generate and save plot as JPG")
    args = parser.parse_args()

    PATHS = load_config("config.ini")

    if args.download or args.connection:
        locations = load_data(PATHS["paths_input"])["locations"]
        locations = data_cleansing.clean_locations(locations)
        data_cleansing.save_data(locations, PATHS["paths_output"]["locations"])
    else:
        locations = load_data(PATHS["paths_output"])["locations"]

    if args.download:
        print("Downloading CSV files...")
        download_csv('data_access/dataplace.ini')
        data_frames = load_data(PATHS["paths_input"])
        # Clean the traffic data
        traffic = data_frames["traffic"]
        traffic = data_cleansing.clean_traffic_data(traffic)
        data_cleansing.save_data(traffic, PATHS["paths_output"]["traffic"])
        # Clean the population data
        population = data_frames["population"]
        population = data_cleansing.clean_population_data(population)
        data_cleansing.save_data(population, PATHS["paths_output"]["population"])
        # Clean the buildings data
        buildings = data_frames["buildings"]
        buildings = data_cleansing.clean_bud_data(buildings)
        data_cleansing.save_data(buildings, PATHS["paths_output"]["buildings"])


    if args.connection:
        print("Processing data form snowflake...")
        dataframes = get_data_frames_from_snowflake('data_access/dataplace.ini')
        # Clean the traffic data
        traffic = dataframes['DATAPLACE_TRAFFIC']
        traffic = data_cleansing.clean_traffic_data(traffic)
        # Clean the population data
        population = dataframes['DATAPLACE_POPULATION']
        population = data_cleansing.clean_population_data(population)
        # Clean the buildings data
        buildings = dataframes["buildings"]
        buildings = data_cleansing.clean_bud_data(buildings)

    else:
        print("Processing data from files...")
        data_frames = load_data(PATHS["paths_output"])
        traffic = data_frames['traffic']
        population = data_frames["population"]
        buildings = data_frames["buildings"]

    traffic_structure_output_file = PATHS['jpg_output']['traffic_structure']
    demographic_structure_output_file = PATHS['jpg_output']['demographic_structure']
    location_for_population_analysis = PATHS['analysis']['location_for_population_analysis']
    buildings_analysis_prefix = PATHS['jpg_output']['buildings_analysis_prefix']


    ## Ensure dataframes are not empty before proceeding
    if not traffic.empty and not locations.empty:
        print("Creating an analysis of:")
        print("1. Movements between given locations:")
        co_visitation.create_matrix(
            traffic,
            locations)
        print("2. Repeatability of mobile signals:")
        repeatability.calculate_and_print_repeat_frequencies(
            traffic,
            locations)
        print("3. Hourly traffic structure:")
        traffic_structure.process_and_plot_traffic_data(
            traffic,
            locations,
            plot=args.plot,
            output_jpg=traffic_structure_output_file)
        print("4. Show the demographic structure:")
        demographic_structure.analyze_and_plot_population_data(
            population,
            locations,
            location_for_population_analysis,
            plot=args.plot,
            output_file=demographic_structure_output_file)
        print("5. Characteristics of buildings:")
        location_names = PATHS['analysis']['location_for_buildings_analysis'].split(',')
        print(location_names)
        buildings_characteristics.analyze_and_display_buildings(
            buildings,
            locations,
            location_names,
            save_to_file=args.plot,
            prefix=buildings_analysis_prefix
            )
    else:
        print("One or more required dataframes are empty. Please check your data files.")
#
if __name__ == "__main__":
    main()
