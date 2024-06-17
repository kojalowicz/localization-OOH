# Data Analyzer

This repository contains a script for data analysis, which includes downloading, processing, and analyzing various datasets. The script supports generating plots and saving results in a PDF format.

## Table of Contents

- [Requirements](#requirements)
- [Installation](#installation)
- [Usage](#usage)
- [Configuration](#configuration)
- [Parameters](#parameters)
- [Description](#description)
- [License](#license)

## Requirements

- Python 3.6+
- pandas
- argparse
- configparser
- shapely
- fpdf
- unidecode
- PIL (Python Imaging Library)

## Installation

1. Clone the repository:

    ```bash
    git clone https://github.com/your-repo/data-analyzer.git
    cd data-analyzer
    ```

2. Install the required Python packages:

    ```bash
    pip install -r requirements.txt
    ```

## Usage

Run the script with the following command:

```bash
python script.py [options]
Configuration
The script uses a configuration file config.ini to manage file paths and analysis parameters. Ensure that config.ini is properly set up before running the script.

Example config.ini

[paths_input]
locations = input/locations.csv
traffic = input/traffic.parquet
population = input/population.csv
buildings = input/buildings.csv

[paths_output]
locations = output/locations.csv
traffic = output/traffic.csv
population = output/population.csv
buildings = output/buildings.csv

[jpg_output]
traffic_structure = output/traffic_structure.jpg
demographic_structure = output/demographic_structure.jpg
buildings_analysis = output/buildings_analysis.jpg
matrix = output/matrix.jpg
top10_visit_frequencies = output/top10_visit_frequencies.jpg

[analysis]
location_for_population_analysis = analysis/location_for_population_analysis.csv
user_work_location = analysis/user_work_location.csv
user_commute_location = analysis/user_commute_location.csv
user_home_location = analysis/user_home_location.csv
location_for_buildings_analysis = location1,location2,location3

Parameters
The script accepts the following command-line parameters:

-d, --download: Download CSV files.
-c, --connection: Use database connection to fetch data.
-p, --plot: Generate and save plots as JPG.
--pdf: Save analysis results in a PDF file. (Implied --plot)
Description
The script performs the following tasks:

Load Configuration: Reads the configuration file to obtain file paths and analysis parameters.
Load Data: Loads data from CSV and Parquet files or from a database connection.
Clean Data: Cleans the loaded data using functions from the data_cleansing module.
Analyze Data:
Creates a co-visitation matrix of traffic data.
Calculates repeat visit frequencies.
Processes and plots hourly traffic structure.
Analyzes and plots demographic structure.
Analyzes characteristics of buildings.
Estimates likely places of residence and work.
Generate Outputs:
Saves plots as JPG files.
Saves analysis results in a PDF file (if specified).
License
This project is licensed under the MIT License.
