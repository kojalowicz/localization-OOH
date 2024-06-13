import snowflake.connector
import pandas as pd
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

def fetch_data(connection, database, schema):
    """
    Fetch list of tables and return DataFrames for each table.

    Parameters:
    connection (snowflake.connector.connection): Snowflake connection object.
    database (str): Database name.
    schema (str): Schema name.

    Returns:
    dict: A dictionary containing DataFrames for each table.
    """
    cursor = connection.cursor()
    dataframes = {}

    try:
        query = f"SHOW TABLES IN SCHEMA {database}.{schema}"
        cursor.execute(query)
        tables = cursor.fetchall()

        for table in tables:
            table_name = table[1]
            query = f"SELECT * FROM {database}.{schema}.{table_name}"
            cursor.execute(query)
            df = pd.DataFrame.from_records(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
            dataframes[table_name] = df
    finally:
        cursor.close()

    return dataframes

def sql_to_dataframes(config_file):
    config = read_config(config_file)
    connection = get_connection(config)

    try:
        dataframes = fetch_data(connection, config.get('DATABASE'), config.get('SCHEMA'))
    finally:
        connection.close()

    return dataframes

if __name__ == "__main__":
    sql_to_dataframes("config.ini")
