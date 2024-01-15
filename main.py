import requests
import pyodbc
from logger import main_logger as logger
import sys
import configparser
from datetime import datetime, timedelta
from utils import truncate_table, insert_eguide_election_data, get_api_data, connect_to_sql_server

config = configparser.ConfigParser()
config.read('config.ini')

if __name__ == "__main__":

    api_endpoint = config["General"]["API_ENDPOINT"]
    api_token = config["General"]["API_TOKEN"]

    db_username = config["Database"]["DB_USERNAME"]
    db_password = config["Database"]["DB_PASSWORD"]
    db_host = config["Database"]["DB_HOST"]
    db_database = config["Database"]["DB_DATABASE"]

    table_to_truncate = 'ElectionGuide'

    if db_username and db_password and db_host and db_database:
        cursor, connection = connect_to_sql_server(
            username=db_username,
            password=db_password,
            server=db_host,
            database=db_database
        )
        if cursor is not None:
            logger.info("Connected to SQL Server successfully!")
    else:
        missing_credentials = []

        if not db_username:
            missing_credentials.append("username")
        if not db_password:
            missing_credentials.append("password")
        if not db_host:
            missing_credentials.append("host")
        if not db_database:
            missing_credentials.append("database")

        print(f"Please make sure the following database credentials are provided: {', '.join(missing_credentials)}")
        sys.exit(1)  # Exit with a non-zero status code to indicate an error

    try:
        logger.info("Election Guide API Script Running")
        logger.info(f"Script Run Date/Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        api_data = get_api_data(api_endpoint, api_token)

        if api_data:
            logger.info("API returned payload is VALID")

            total_election_ids = len(api_data)
            last_week = datetime.today() - timedelta(days=7)

            election_ids_starting_from_last_week = [
                record['election_id']
                for record in api_data
                if datetime.strptime(record.get("date_updated"), '%Y-%m-%dT%H:%M:%S.%fZ') >= last_week
            ]

            election_ids_before_last_week = [
                record['election_id']
                for record in api_data
                if datetime.strptime(record.get("date_updated"), '%Y-%m-%dT%H:%M:%S.%fZ') < last_week
            ]

            logger.info(f"Found Total of {total_election_ids} unique Election IDs")
            truncate_table(cursor, table_to_truncate)
            logger.info("Processing Election Guide JSON payload data")
            logger.info(
                f"Found {len(election_ids_starting_from_last_week)} Election IDs starting from previous week of {last_week}"
            )
            logger.info(
                f"Found {len(election_ids_before_last_week)} Election IDs with Election Dates before previous week of {last_week}"
            )
            logger.info(f"Inserting Total of {len(election_ids_starting_from_last_week)} unique Election IDs")
            for record in api_data:
                record_pub_date = datetime.strptime(record.get("date_updated"), '%Y-%m-%dT%H:%M:%S.%fZ')
                if record_pub_date >= last_week:
                    insert_eguide_election_data(cursor, record)
                    logger.info(
                        f"Inserting Election ID {record['election_id']} – {record['election_name']['en_US']} – {record_pub_date.strftime('%Y-%m-%d %H:%M:%S')}"
                    )

            cursor.commit()
            logger.info("Election Guide Data Successfully Inserted")

        else:
            logger.info("Script Terminated")
            sys.exit(1)
    except Exception as ex:
        logger.error(f"An error occurred {ex}")
    finally:
        cursor.close()
        logger.info(f"Terminating Script – {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
