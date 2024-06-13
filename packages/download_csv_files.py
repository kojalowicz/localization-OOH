import snowflake.connector
import pandas as pd
import os
from urllib.parse import urlparse
from configparser import ConfigParser

def read_config(config_file):
    """
    Read configuration from a file.

    Parameters:
    config_file (str): Path to the configuration file.

    Returns:
    ConfigParser: Configuration object.
    """
    config = ConfigParser()
    config.read(config_file)
    return config['Snowflake']

def save_table_to_csv(cursor, table_name, schema, database, output_dir):
    """
    Save table data to a CSV file.

    Parameters:
    cursor (snowflake.connector.cursor): Snowflake cursor object.
    table_name (str): Name of the table to save.
    schema (str): Schema of the table.
    database (str): Database of the table.
    output_dir (str): Directory to save the CSV file.
    """
    query = f"SELECT * FROM {database}.{schema}.{table_name}"
    cursor.execute(query)
    df = pd.DataFrame.from_records(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
    file_path = os.path.join(output_dir, f"{table_name}.csv")
    df.to_csv(file_path, index=False)
    print(f"Saved {table_name} to {file_path}")

def get_connection(config):
    """
    Establish a connection to Snowflake.

    Parameters:
    config (ConfigParser): Configuration object.

    Returns:
    snowflake.connector.connection: Snowflake connection object.
    """
    USER = config.get('USER')
    PASSWORD = config.get('PASSWORD')
    ACCOUNT = config.get('ACCOUNT')
    REGION = config.get('REGION')
    DATABASE = config.get('DATABASE')
    SCHEMA = config.get('SCHEMA')

    url = f'snowflake://{USER}:{PASSWORD}@{ACCOUNT}.{REGION}.azure/{DATABASE}/{SCHEMA}'
    parsed_url = urlparse(url)

    user = parsed_url.username
    password = parsed_url.password
    account_region = parsed_url.hostname.split('.')
    account = account_region[0]
    region = account_region[1]
    database = parsed_url.path.split('/')[1]
    schema = parsed_url.path.split('/')[2]

    return snowflake.connector.connect(
        user=user,
        password=password,
        account=f"{account}.{region}.azure",
        database=database,
        schema=schema
    )

def fetch_and_save_tables(connection, database, schema, output_dir):
    """
    Fetch list of tables and save each to a CSV file.

    Parameters:
    connection (snowflake.connector.connection): Snowflake connection object.
    database (str): Database name.
    schema (str): Schema name.
    output_dir (str): Directory to save the CSV files.
    """
    cursor = connection.cursor()
    os.makedirs(output_dir, exist_ok=True)

    try:
        query = f"SHOW TABLES IN SCHEMA {database}.{schema}"
        cursor.execute(query)
        tables = cursor.fetchall()

        for table in tables:
            table_name = table[1]
            save_table_to_csv(cursor, table_name, schema, database, output_dir)
    finally:
        cursor.close()

def download_csv_files(config_file):
    config = read_config(config_file)
    connection = get_connection(config)

    try:
        fetch_and_save_tables(connection, config.get('DATABASE'), config.get('SCHEMA'), "data")
    finally:
        connection.close()

if __name__ == "__main__":
    download_csv_files("config.ini")
