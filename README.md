# Election Guide API Integration

## Overview
This Python script integrates with the Election Guide API to fetch election data and insert it into a Microsoft SQL Server database. The script is designed to run periodically to keep the election data up-to-date in the database.

## Requirements
- Python 3.10
- Packages listed in requirements.txt

## Installation
1. Install the required packages using pip:
    ```bash
    pip install -r requirements.txt
    ```
   
## Configuration
1. Create or Update config.ini file with the following structure and fill in the appropriate values:

    ```ini
    [Database]
    DB_USERNAME = your_database_username
    DB_PASSWORD = your_database_password
    DB_HOST = your_database_host
    DB_DATABASE = your_database_name
    DB_TYPE = mysql / mssql
   
    [General]
    LOG_PATH = ./election_guide.log
    API_ENDPOINT = https://electionguide.org/api/v2/elections_demo/
    API_TOKEN = your_api_token
    ```
    Update the API_ENDPOINT and API_TOKEN values with your Election Guide API details.

## Usage

1. Run the script by executing the following command:
    ```bash
    python main.py
    ```
    The script will fetch data from the Election Guide API, truncate the existing data in the specified database table, and insert the new data.
    
## Logging
The script logs its activities to the election_guide.log file. Check this file for information about the script's execution and any errors that may occur.
