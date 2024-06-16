import pandas as pd
import pygeos
import matplotlib.pyplot as plt
import seaborn as sns
import re
from unidecode import unidecode

def analyze_and_display_buildings(building_data, locations, selected_locations, save_to_file=False, prefix=""):
    analysis_results = {}
    for location_name in selected_locations:
        if location_name in locations['location'].values:
            buildings_in_location = analyze_buildings_in_location(building_data, locations, location_name)
            analysis_results[location_name] = buildings_in_location
            print(f"\nAnalysis Result for {location_name}:")
            print(buildings_in_location)

            # Generate and save analysis plot if save_to_file is True
            if save_to_file:
                save_analysis_plot(buildings_in_location, location_name, prefix)
        else:
            print(f"Location '{location_name}' not found in locations data.")

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

def save_analysis_plot(buildings_in_location, location_name, prefix=""):
    # Convert CamelCase to words with spaces before capital letters
    def camelcase_to_words(text):
        # Regular expression to split capital letters, handling 2 or more capital letters
        text = re.sub(r'([a-z])([A-Z])', r'\1 \2', text)
        text = re.sub(r'([A-Z])([A-Z][a-z])', r'\1 \2', text)
        text = re.sub(r'([A-Z])([A-Z])', r'\1 \2', text)
        return text.lower()

    # Apply the function to the FUNOGOLNABUDYNKU_DESC column
    buildings_in_location['FUNOGOLNABUDYNKU_DESC'] = buildings_in_location['FUNOGOLNABUDYNKU_DESC'].apply(camelcase_to_words)

    plt.figure(figsize=(10, 6))
    ax = sns.countplot(y='FUNOGOLNABUDYNKU_DESC', data=buildings_in_location, order=buildings_in_location['FUNOGOLNABUDYNKU_DESC'].value_counts().index)

    # Add the number at the end of each bar
    for p in ax.patches:
        ax.annotate(f"{int(p.get_width())}", (p.get_width(), p.get_y() + p.get_height() / 2), ha='left', va='center')

    # Remove values from the x-axis
    ax.set_xticklabels([])

    # Total number of buildings
    total_buildings = buildings_in_location.shape[0]

    # Updated plot title
    plt.title(f"Building Types Distribution in {location_name} (Total: {total_buildings} buildings)")
    plt.xlabel("")
    plt.ylabel("Building Type")

    plt.tight_layout()

    # Prepare filename without Polish characters and spaces replaced by underscores
    filename = f"{prefix}{unidecode(location_name).replace(' ', '_')}.jpg"

    # Save plot to file
    plt.savefig(filename)
    print(f"Saved analysis plot as {filename}")
    plt.close()

if __name__ == "__main__":
    building_data = pd.read_csv('building_data.csv')
    locations = pd.read_parquet('locations.parquet')

    # List of selected locations for analysis
    selected_locations = ["Mordor na Domaniewskiej", "Osiedle Wilanów"]

    # Analyze and display results for selected locations with the option to save to file
    analyze_and_display_buildings(building_data, locations, selected_locations, save_to_file=True, prefix="analysis_")
