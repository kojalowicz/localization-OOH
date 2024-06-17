import pandas as pd
import pygeos
import matplotlib.pyplot as plt
import seaborn as sns
import re

def analyze_and_display_buildings(building_data, locations, selected_locations, save_to_file=False, output_file=""):
    analysis_results = {}

    for location_name in selected_locations:
        if location_name in locations['location'].values:
            buildings_in_location = analyze_buildings_in_location(building_data, locations, location_name)
            analysis_results[location_name] = (buildings_in_location, buildings_in_location.shape[0])

            print(f"\nAnalysis Result for {location_name}:")
            print(buildings_in_location)
        else:
            print(f"Location '{location_name}' not found in locations data.")

    if save_to_file:
        merge_and_save_plots(analysis_results, output_file)

    return analysis_results

def analyze_buildings_in_location(building_data, locations, location_name):
    buildings_in_location = filter_buildings_by_location(building_data, locations, location_name)
    average_area = buildings_in_location['AREA'].mean()
    building_counts = buildings_in_location['FUNOGOLNABUDYNKU_DESC'].value_counts()
    average_floors = buildings_in_location['LICZBAKONDYGNACJI'].mean()

    print(f"\nBuilding Analysis for {location_name}:")
    print(f"Average Area: {average_area:.2f} sqm")
    print("Number of Buildings per Type:")
    print(building_counts)
    print(f"Average Number of Floors: {average_floors:.2f}")

    return buildings_in_location

def filter_buildings_by_location(building_data, locations, location_name):
    # Select location geometry for the specified location
    location_geometry_bytes = locations.loc[locations['location'] == location_name, 'geometry'].values[0]

    # Convert binary geometry to a PyGEOS geometry
    location_geometry = pygeos.Geometry(location_geometry_bytes)

    # Filter building data based on whether each building is within the location shape
    def is_within_location(row):
        building_geometry = pygeos.Geometry(row['GEOMETRY'])
        return pygeos.intersects(building_geometry, location_geometry)

    buildings_in_location = building_data[building_data.apply(is_within_location, axis=1)]

    return buildings_in_location

def save_analysis_plot(ax, buildings_in_location, location_name, total_buildings):
    # Convert CamelCase to words with spaces before capital letters
    def camelcase_to_words(text):
        text = re.sub(r'([a-z])([A-Z])', r'\1 \2', text)
        text = re.sub(r'([A-Z])([A-Z][a-z])', r'\1 \2', text)
        text = re.sub(r'([A-Z])([A-Z])', r'\1 \2', text)
        return text.lower()

    ax.set_title(f"Building Types Distribution in {location_name} (Total: {total_buildings} buildings)")
    ax.set_xlabel("")
    ax.set_ylabel("Building Type")
    ax.set_xticklabels([])

    # Add the number at the end of each bar
    for p in ax.patches:
        ax.annotate(f"{int(p.get_width())}", (p.get_width(), p.get_y() + p.get_height() / 2), ha='left', va='center')

def merge_and_save_plots(locations_analysis_results, output_file=""):
    fig, axs = plt.subplots(len(locations_analysis_results), figsize=(12, 8))

    for i, (location_name, (buildings_in_location, total_buildings)) in enumerate(locations_analysis_results.items()):
        ax = axs[i] if len(locations_analysis_results) > 1 else axs
        sns.countplot(y='FUNOGOLNABUDYNKU_DESC', data=buildings_in_location,
                      order=buildings_in_location['FUNOGOLNABUDYNKU_DESC'].value_counts().index, ax=ax)
        save_analysis_plot(ax, buildings_in_location, location_name, total_buildings)

    plt.tight_layout()

    # Prepare filename without Polish characters and spaces replaced by underscores
    filename = output_file
    plt.savefig(filename)
    print(f"Saved combined analysis plot as {filename}")
    plt.close()

if __name__ == "__main__":
    building_data = pd.read_csv('building_data.csv')
    locations = pd.read_parquet('locations.parquet')

    # List of selected locations for analysis
    selected_locations = ["Mordor na Domaniewskiej", "Osiedle Wilanów"]

    # Analyze and display results for selected locations with the option to save to file
    analyze_and_display_buildings(building_data, locations, selected_locations, save_to_file=True, prefix="analysis_")
