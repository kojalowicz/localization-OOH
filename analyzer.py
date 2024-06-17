import pandas as pd
import argparse
import configparser
from shapely import wkb
from packages import (snowflake_data_handler, snowflake_csv_saver, co_visitation, data_cleansing, repeatability,
                      traffic_structure, demographic_structure, buildings_characteristics, residence_work)
from fpdf import FPDF
from unidecode import unidecode

def append_to_pdf(pdf, content):
    if isinstance(content, str):
        pdf.set_font("Arial", size=12)
        pdf.add_page()
        pdf.multi_cell(0, 10, txt=content)
        pdf.ln()
    elif isinstance(content, dict) and 'path' in content and content['path'].lower().endswith('.jpg'):
        img_path = content['path']

        # Calculate maximum image dimensions that fit the page
        max_width = pdf.w - 20
        max_height = pdf.h - 20  # Adjust as needed for margins

        # Get image dimensions using PIL (Python Imaging Library)
        from PIL import Image
        img = Image.open(img_path)
        img_width, img_height = img.size

        # Calculate scaling factors to fit the image within page dimensions
        width_ratio = max_width / img_width
        height_ratio = max_height / img_height

        # Use the smaller ratio to ensure the entire image fits within the page
        scale_factor = min(width_ratio, height_ratio)

        # Calculate resized dimensions
        new_width = img_width * scale_factor
        new_height = img_height * scale_factor

        # Calculate position to center image
        x = (pdf.w - new_width) / 2
        y = (pdf.h - new_height) / 2

        # Add a new page if no page is open
        if pdf.page_no() == 0:
            pdf.add_page()

        # Add image to PDF
        pdf.image(img_path, x=x, y=y, w=new_width, h=new_height)


def create_pdf(contents, output_file):
    pdf = FPDF()
    new_page_added = False  # Flag to track if a new page has been added

    for content in contents:
        if isinstance(content, dict) and 'path' in content and isinstance(content['path'], str) and content['path'].lower().endswith('.jpg'):
            pdf.add_page()  # Add a new page before adding the image
            append_to_pdf(pdf, content)
        elif content == 'new_page':
            pdf.add_page()
        else:
            append_to_pdf(pdf, content)

    pdf.output(output_file)


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

def download_csv(data_access, input):
    snowflake_csv_saver.download_csv_files(data_access, input)

def get_data_frames_from_snowflake(data_access):
    return snowflake_data_handler.sql_to_dataframes(data_access)

def main():
    parser = argparse.ArgumentParser(description="Data Analyzer")
    parser.add_argument("-d", "--download", action="store_true", help="Download CSV files")
    parser.add_argument("-c", "--connection", action="store_true", help="Use database connection")
    parser.add_argument("-p", "--plot", action="store_true", help="Generate and save plot as JPG")
    parser.add_argument("--pdf", action="store_true", help="Save analysis to PDF")
    args = parser.parse_args()

    if args.pdf:
        args.plot=True

    PATHS = load_config("config.ini")

    if args.download:
        print("Downloading CSV files...")
        download_csv('data_access/dataplace.ini', "input")
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

    if args.download or args.connection:
        locations = load_data(PATHS["paths_input"])["locations"]
        locations = data_cleansing.clean_locations(locations)
        data_cleansing.save_data(locations, PATHS["paths_output"]["locations"])
    else:
        locations = load_data(PATHS["paths_output"])["locations"]

    # Defining variables
    traffic_structure_output_file = PATHS['jpg_output']['traffic_structure']
    demographic_structure_output_file = PATHS['jpg_output']['demographic_structure']
    location_for_population_analysis = PATHS['analysis']['location_for_population_analysis']
    buildings_analysis = PATHS['jpg_output']['buildings_analysis']
    matrix_jpg = PATHS['jpg_output']['matrix']
    top10_visit_frequencies_jpg = PATHS['jpg_output']['top10_visit_frequencies']
    work_location = PATHS['analysis']['user_work_location']
    commute_location = PATHS['analysis']['user_commute_location']
    home_location = PATHS['analysis']['user_home_location']

    content_for_pdg = []
    ## Ensure dataframes are not empty before proceeding
    if not traffic.empty and not locations.empty and not buildings.empty and not population.empty:

        print("Creating an analysis of:")
        print("Movements between given locations:")
        matrix = co_visitation.create_matrix(
            traffic,
            locations,
            plot=args.plot,
            output_file=matrix_jpg)
        print(matrix)
        content_for_pdg.append({'path': matrix_jpg})

        print("Repeatability of mobile signals:")
        repeat_visits_summary, visit_frequency_summary_df  = repeatability.calculate_and_return_repeat_frequencies(
            traffic,
            locations)
        repeatability.generate_combined_top10_plot(repeat_visits_summary, visit_frequency_summary_df, top10_visit_frequencies_jpg)
        content_for_pdg.append({'path': top10_visit_frequencies_jpg})
        print(visit_frequency_summary_df)

        print("Hourly traffic structure:")
        traffic_structure.process_and_plot_traffic_data(
            traffic,
            locations,
            plot=args.plot,
            output_jpg=traffic_structure_output_file)
        content_for_pdg.append({'path': traffic_structure_output_file})

        print("Show the demographic structure:")
        demographic_structure.analyze_and_plot_population_data(
            population,
            locations,
            location_for_population_analysis,
            plot=args.plot,
            output_file=demographic_structure_output_file)
        content_for_pdg.append({'path': demographic_structure_output_file})

        print("Characteristics of buildings:")
        location_names = PATHS['analysis']['location_for_buildings_analysis'].split(',')
        print(location_names)
        print(content_for_pdg[-1])
        buildings_characteristics.analyze_and_display_buildings(
            buildings,
            locations,
            location_names,
            save_to_file=args.plot,
            output_file=buildings_analysis
        )

        content_for_pdg.append({'path': buildings_analysis})

        # Estimating the likely place of residence and work
        content_for_pdg.append(f"Estimating the likely place of residence and work\n")

        work_estimation = residence_work.analyze_travel_and_users(
            traffic,
            locations,
            commute_location,
            work_location,
            8, 18
        )

        content_for_pdg[-1] = content_for_pdg[-1] + f"\nEstimated number users commuting between {work_location} and {commute_location} and probably works in {work_location}:\n"
        content_for_pdg[-1] = content_for_pdg[-1] + str(len(work_estimation['estimated_locations'])) + f'\n'

        residence_estimation = residence_work.analyze_travel_and_users(
            traffic,
            locations,
            commute_location,
            home_location,
            22, 5
        )

        content_for_pdg[-1] = content_for_pdg[-1] + f"\nEstimated number users commuting between {home_location} and {commute_location} and probably living in {home_location}:\n"
        content_for_pdg[-1] = content_for_pdg[-1] + str(len(residence_estimation['estimated_locations'])) + f'\n'
        print(content_for_pdg[-1])
    else:
        print("One or more required dataframes are empty. Please check your data files.")

    if args.pdf:
        create_pdf(content_for_pdg, 'analysis.pdf')

if __name__ == "__main__":
    main()
