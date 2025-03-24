```markdown
# NBA Player Statistics ETL Pipeline

## Overview
This project is an ETL (Extract, Transform, Load) pipeline that fetches NBA player statistics from the [RAPIDAPI NBA API](https://api-nba-v1.p.rapidapi.com/), processes the data, and stores it in a MySQL database.

## Features
- Fetches player statistics via an API request.
- Parses and processes the data to calculate player averages.
- Stores the results in a MySQL database.
- Handles API errors and database connection issues.

## Technologies Used
- Python
- Pandas
- MySQL
- Requests (HTTP API requests)
- dotenv (Environment variable management)

## Prerequisites
Before running the project, ensure you have the following:

- Python installed (>=3.8)
- MySQL database set up
- API Key from [RAPIDAPI](https://rapidapi.com/)
- Required Python libraries installed (see below for installation)

## Installation

1. Clone this repository:
   ```sh
   git clone https://github.com/your-repo/nba-stats-etl.git
   cd nba-stats-etl
   ```
2. Install dependencies:
   ```sh
   pip install -r requirements.txt
   ```
3. Set up your `.env` file with the following variables:
   ```env
   RAPIDAPI_KEY=your_rapidapi_key
   HOST=your_mysql_host
   MYSQL_DATABASE=your_database_name
   MYSQL_USERNAME=your_mysql_username
   MYSQL_PASSWORD=your_mysql_password
   ```

## Usage

1. Run the script:
   ```sh
   python main.py
   ```
2. The script will:
   - Fetch player statistics from the API.
   - Process the data and compute player averages.
   - Store the data in the MySQL database.

## Database Schema
The processed data is stored in a MySQL table named `player_averages` with the following schema:

```sql
CREATE TABLE IF NOT EXISTS player_averages (
    player VARCHAR(255) PRIMARY KEY,
    avg_points FLOAT,
    avg_rebounds FLOAT,
    avg_assists FLOAT,
    total_games INT
);
```

## Error Handling
- API request errors (e.g., HTTP errors, timeout, or connection issues) are logged.
- Database connection errors are captured and printed.

## Author
Alan Zhou
