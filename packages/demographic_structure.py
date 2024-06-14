import pandas as pd
from shapely.geometry import Point, shape
from shapely import wkb
import matplotlib.pyplot as plt

def analyze_age_structure(population_data):
    # Calculate the total population
    population_data['TOTAL'] = population_data['FEMALE'] + population_data['MALE']

    # Calculate the percentage structure for each age group
    total_population = population_data['TOTAL'].sum()
    male_population = population_data['MALE'].sum()
    female_population = population_data['FEMALE'].sum()

    female_percentage = (female_population / total_population) * 100
    male_percentage = (male_population / total_population) * 100

    population_data['FEMALE_PERCENTAGE'] = (population_data['FEMALE'] / population_data['TOTAL']) * 100
    population_data['MALE_PERCENTAGE'] = (population_data['MALE'] / population_data['TOTAL']) * 100

    return {"population_data": population_data, "female_percentage": female_percentage, "male_percentage": male_percentage}

def filter_population_by_location(population_data, locations, location_name):
    # Select location geometry for the specified location
    location_geometry = locations.loc[locations['location'] == location_name, 'geometry'].values[0]

    # Convert binary geometry to a Shapely shape
    location_shape = wkb.loads(location_geometry)

    # Filter population data based on whether each point is within the location shape
    def is_within_location(row):
        point = Point(row['LNG'], row['LAT'])
        return point.within(location_shape)

    population_data_filtered = population_data[population_data.apply(is_within_location, axis=1)]

    # Add location information to the filtered population data
    population_data_filtered = population_data_filtered.copy()  # Avoid SettingWithCopyWarning
    population_data_filtered['GEOMETRY'] = location_geometry
    population_data_filtered['LOCATION_NAME'] = location_name

    return population_data_filtered

def analyze_population_in_location(population_data, locations, location_name):
    # Filter population data for location
    population_data = filter_population_by_location(population_data, locations, location_name)

    # Analyze age structure for location
    analyzed_population = analyze_age_structure(population_data)

    # Add location information to the analyzed population data
    analyzed_population["location"] = {
        "location_name": location_name,
        "LNG": locations.loc[locations['location'] == location_name, 'lng'].values[0],
        "LAT": locations.loc[locations['location'] == location_name, 'lat'].values[0],
        "GEOMETRY": analyzed_population["population_data"].iloc[0]['GEOMETRY']
    }
    return analyzed_population

def summarize_population_data(analyzed_population_data):
    # Summarize the analyzed population data to a single row
    summarized_data = analyzed_population_data["population_data"].sum().to_frame().T

    # Set the total and percentages for the summarized row
    summarized_data['TOTAL'] = summarized_data['FEMALE'] + summarized_data['MALE']
    summarized_data['FEMALE_PERCENTAGE'] = (summarized_data['FEMALE'] / summarized_data['TOTAL']) * 100
    summarized_data['MALE_PERCENTAGE'] = (summarized_data['MALE'] / summarized_data['TOTAL']) * 100

    # Add location information to the summarized data from analyzed_population_data['location']
    summarized_data['GEOMETRY'] = analyzed_population_data['location']['GEOMETRY']
    summarized_data['LOCATION_NAME'] = analyzed_population_data['location']['location_name']
    summarized_data['LNG'] = analyzed_population_data['location']['LNG']
    summarized_data['LAT'] = analyzed_population_data['location']['LAT']

    return summarized_data

def analyze_demographic_distribution(summarized_data):
    # Extract relevant columns for age and gender analysis, excluding percentage columns
    age_gender_columns = [col for col in summarized_data.columns if (col.startswith('FEMALE') or col.startswith('MALE')) and 'PERCENTAGE' not in col]

    # Calculate the total population for each age group and gender
    total_population = summarized_data['TOTAL'].iloc[0]

    # Calculate the percentage for each group
    percentages = {}
    for col in age_gender_columns:
        percentages[col] = (summarized_data[col].iloc[0] / total_population) * 100

    return percentages



def plot_demographic_pyramid(demographic_distribution, location_name, output_file='demographic_pyramid.jpg'):
    # Separate age groups for male and female, excluding total columns
    female_groups = {k: v for k, v in demographic_distribution.items() if k.startswith('FEMALE') and 'TOTAL' not in k}
    male_groups = {k: v for k, v in demographic_distribution.items() if k.startswith('MALE') and 'TOTAL' not in k}

    # Calculate total female and male populations
    total_female = female_groups.pop('FEMALE', None)
    total_male = male_groups.pop('MALE', None)

    # Map age ranges to new labels
    age_range_mapping = {
        'FEMALE0003': '00 - 03',
        'FEMALE0307': '03 - 07',
        'FEMALE0812': '08 - 12',
        'FEMALE1315': '13 - 15',
        'FEMALE1618': '16 - 18',
        'FEMALE1924': '19 - 24',
        'FEMALE2529': '25 - 29',
        'FEMALE3034': '30 - 34',
        'FEMALE3539': '35 - 39',
        'FEMALE4044': '40 - 44',
        'FEMALE4549': '45 - 49',
        'FEMALE5054': '50 - 54',
        'FEMALE5559': '55 - 59',
        'FEMALE6064': '60 - 64',
        'FEMALE6569': '65 - 69',
        'FEMALE7074': '70 - 74',
        'FEMALE7579': '75 - 79',
        'FEMALE8084': '80 - 84',
        'FEMALE8589': '85 - 89',
        'FEMALE9099': '90 - 99'
    }

    # Replace old labels with new labels
    female_groups = {age_range_mapping.get(k, k): v for k, v in female_groups.items()}
    male_groups = {age_range_mapping.get(k, k): v for k, v in male_groups.items()}

    # Convert to lists for plotting
    female_labels = list(female_groups.keys())
    female_values = list(female_groups.values())
    male_labels = list(male_groups.keys())
    male_values = list(male_groups.values())

    # Convert male values to negative for the pyramid plot
    male_values = [-v for v in male_values]

    # Plot the demographic pyramid
    fig, ax = plt.subplots(figsize=(10, 8))

    y = range(len(female_labels))

    ax.barh(y, female_values, color='pink', label='Female')
    ax.barh(y, male_values, color='lightblue', label='Male')

    # Add total female and male to the legend
    legend_labels = ['Female', 'Male']
    legend_handles = [plt.Rectangle((0, 0), 1, 1, color='pink'), plt.Rectangle((0, 0), 1, 1, color='lightblue')]
    if total_female is not None:
        legend_labels.insert(0, f'Total Female: {total_female:.0f}%')
    if total_male is not None:
        legend_labels.insert(1, f'Total Male: {total_male:.0f}%')

    ax.set_yticks(y)
    ax.set_yticklabels(female_labels)
    ax.invert_yaxis()
    ax.set_xlabel('Percentage')
    ax.set_title(f'Demographic Pyramid for {location_name}')

    # Add percentage values to bars
    for i, (value, label) in enumerate(zip(female_values, female_labels)):
        ax.text(value, i, f'{value:.1f}%', ha='left', va='center', color='black', fontsize=10)

    for i, (value, label) in enumerate(zip(male_values, male_labels)):
        ax.text(value, i, f'{abs(value):.1f}%', ha='right', va='center', color='black', fontsize=10)

    # Add the legend
    ax.legend(legend_handles, legend_labels)

    # Remove X-axis ticks and labels
    ax.set_xticks([])
    ax.set_xticklabels([])

    # Save the plot to a JPG file
    plt.tight_layout()
    plt.savefig(output_file)
    plt.close()

def analyze_and_plot_population_data(population_data, locations, location_name, output_file='demographic_pyramid.jpg', plot=True):
    # Analyze population data for location
    analyzed_population_data = analyze_population_in_location(population_data, locations, location_name)

    # Summarize analyzed population data
    summarized_data = summarize_population_data(analyzed_population_data)

    # Display analysis results and get demographic distribution
    demographic_distribution = analyze_demographic_distribution(summarized_data)
    print(f"Demographic Distribution for {location_name}:")
    for k, v in demographic_distribution.items():
        print(f'{k}: {v} %')

    # Plot demographic pyramid if plot is True
    if plot:
        plot_demographic_pyramid(demographic_distribution, location_name, output_file)

    return demographic_distribution

if __name__ == "__main__":
    # Load population data from CSV file
    population_file = 'POPULATION.csv'
    population_data = pd.read_csv(population_file)

    # Load locations data from another file (assuming it contains location geometry in binary format)
    locations_file = 'locations.parquet'
    locations = pd.read_parquet(locations_file)

    # Analyze and optionally plot population data for Osiedle Wilanów
    analyze_and_plot_population_data(population_data, locations, 'Osiedle Wilanów')
