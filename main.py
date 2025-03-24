import os
import requests
import pandas as pd
from datetime import datetime
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv

load_dotenv()

# Debug: Print environment variables
print(f"API Key: {os.getenv('RAPIDAPI_KEY')}")
print(f"MySQL Host: {os.getenv('HOST')}")
print(f"MySQL Database: {os.getenv('MYSQL_DATABASE')}")
print(f"MySQL Username: {os.getenv('MYSQL_USERNAME')}")
print(f"MySQL Password: {os.getenv('MYSQL_PASSWORD')}")

# Load API key to make API requests
RAPIDAPI_KEY = os.getenv('RAPIDAPI_KEY')

headers = {
    'x-rapidapi-key': RAPIDAPI_KEY,
    'x-rapidapi-host': "api-nba-v1.p.rapidapi.com"
}

url = "https://api-nba-v1.p.rapidapi.com/players/statistics"
params = {"id": "236", "season": "2020"}

def get_player_stats(url, headers, params):
    """
    Fetch player statistics using the API
    """
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        print(data)  # Debug: Print the API response
        return data
    except requests.exceptions.HTTPError as http_error_message:
        print(f"[HTTP ERROR]: {http_error_message}")

    except requests.exceptions.ConnectionError as connection_error_message:
        print(f" [CONNECTION ERROR]: {connection_error_message}")

    except requests.exceptions.Timeout as timeout_error_message:
        print(f" [TIMEOUT ERROR]: {timeout_error_message}")

    except requests.exceptions.RequestException as other_error_message:
        print(f" [UNKNOWN ERROR]: {other_error_message}")

def process_player_stats(data):
    """
    Parse JSON data and calculate total averages for the player
    """
    player_stats = []
    total_points = 0
    total_rebounds = 0
    total_assists = 0
    total_games = 0

    for player_data in data['response']:
        player = player_data.get('player', {})
        statistics = player_data

        # Skip if required keys are missing
        if not player:
            continue

        # Extract relevant data
        player_name = player.get('firstname', '') + " " + player.get('lastname', '')
        points = int(statistics.get('points', 0))
        rebounds = int(statistics.get('totReb', 0))  # Use 'totReb' for total rebounds
        assists = int(statistics.get('assists', 0))

        # Accumulate totals
        total_points += points
        total_rebounds += rebounds
        total_assists += assists
        total_games += 1

    # Calculate averages
    avg_points = total_points / total_games if total_games > 0 else 0
    avg_rebounds = total_rebounds / total_games if total_games > 0 else 0
    avg_assists = total_assists / total_games if total_games > 0 else 0

    # Append data 
    player_stats.append({
        'player': player_name,
        'avg_points': avg_points,
        'avg_rebounds': avg_rebounds,
        'avg_assists': avg_assists,
        'total_games': total_games
    })

    return player_stats

def create_dataframe(player_stats):
    """
    Convert list of dictionaries into a Pandas dataframe
    """
    df = pd.DataFrame(player_stats)
    return df

HOST = os.getenv('HOST')
MYSQL_DATABASE = os.getenv('MYSQL_DATABASE')
MYSQL_USERNAME = os.getenv('MYSQL_USERNAME')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD')

def create_db_connection(host_name, user_name, user_password, db_name):
    """
    Establish a connection to the MySQL database
    """
    db_connection = None
    try:
        db_connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name,
            auth_plugin='mysql_native_password'  # Specify the authentication plugin
        )
        print("MySQL Database connection successful")
    except Error as e:
        print(f" [DATABASE CONNECTION ERROR]: '{e}'")
    
    return db_connection

def create_table(db_connection):
    """
    Create a table if it does not exist in the MySQL database
    """
    CREATE_TABLE_SQL_QUERY = """
    CREATE TABLE IF NOT EXISTS player_averages (
        `player` VARCHAR(255),
        `avg_points` FLOAT,
        `avg_rebounds` FLOAT,
        `avg_assists` FLOAT,
        `total_games` INT,
        PRIMARY KEY (`player`)
    );
    """
    try:
        cursor = db_connection.cursor()
        cursor.execute(CREATE_TABLE_SQL_QUERY)
        db_connection.commit()
        print("Table created successfully ✅")

    except Error as e:
        print(f" [CREATING TABLE ERROR]: '{e}'")


def insert_into_table(db_connection, df):
    """
    Insert or update the player averages data in the database from the dataframe
    """
    cursor = db_connection.cursor()

    INSERT_DATA_SQL_QUERY = """
    INSERT INTO player_averages (`player`, `avg_points`, `avg_rebounds`, `avg_assists`, `total_games`)
    VALUES (%s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        `avg_points` = VALUES(`avg_points`),
        `avg_rebounds` = VALUES(`avg_rebounds`),
        `avg_assists` = VALUES(`avg_assists`),
        `total_games` = VALUES(`total_games`)
    """
    # Create a list of tuples from the dataframe values
    data_values_as_tuples = [tuple(x) for x in df.to_numpy()]

    # Execute the query
    cursor.executemany(INSERT_DATA_SQL_QUERY, data_values_as_tuples)
    db_connection.commit()
    print("Data inserted or updated successfully ✅")


def run_data_pipeline():
    """
    Execute the ETL pipeline
    """

    data = get_player_stats(url, headers, params)

    if data and 'response' in data and data['response']:
        player_stats = process_player_stats(data)
        df = create_dataframe(player_stats)
        print(df.to_string(index=False))

    else:
        print("No data available or an error occurred")

    db_connection = create_db_connection(HOST, MYSQL_USERNAME, MYSQL_PASSWORD, MYSQL_DATABASE)

    # If connection is successful, proceed with creating table and inserting data
    if db_connection is not None:
        create_table(db_connection)
        df = create_dataframe(player_stats)
        insert_into_table(db_connection, df)


if __name__ == "__main__":
    run_data_pipeline()