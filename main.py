import requests
import pyodbc
from logger import main_logger as logger
import sys
import configparser
from datetime import datetime, timedelta
from utils import truncate_table, insert_eguide_election_data, get_api_data, connect_to_sql_server, \
    connect_to_mysql_server, create_table_if_not_exists_mysql, create_table_if_not_exists_sql_server

config = configparser.ConfigParser()
config.read('config.ini')

if __name__ == "__main__":
    total_inserted = 0
    api_endpoint = config["General"]["API_ENDPOINT"]
    api_token = config["General"]["API_TOKEN"]

    db_username = config["Database"]["DB_USERNAME"]
    db_password = config["Database"]["DB_PASSWORD"]
    db_host = config["Database"]["DB_HOST"]
    db_database = config["Database"]["DB_DATABASE"]
    db_type = config["Database"]["DB_TYPE"]

    table_to_truncate = 'ElectionGuide'

    if db_username and db_password and db_host and db_database:
        if db_type == 'mssql':
            cursor, connection = connect_to_sql_server(
                username=db_username,
                password=db_password,
                server=db_host,
                database=db_database
            )
            if cursor is not None:
                logger.info("Connected to SQL Server successfully!")
            else:
                sys.exit(1)
        elif db_type == 'mysql':
            cursor, connection = connect_to_mysql_server(
                user=db_username,
                password=db_password,
                host=db_host,
                database=db_database
            )
            if cursor is not None:
                logger.info("Connected to MYSQL successfully!")
            else:
                logger.error(f"Connection to MYSQL failed")
                sys.exit(1)
        else:
            logger.error("Invalid database type, only mssql and mysql")
            sys.exit(1)
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
        if db_type == "mssql":
            create_table_if_not_exists_sql_server(cursor)
        elif db_type == "mysql":
            create_table_if_not_exists_mysql(cursor)
        else:
            logger.error("Invalid database type, only mssql and mysql")
            sys.exit(1)
        api_data = get_api_data(api_endpoint, api_token)

        if api_data:
            logger.info("API returned payload is VALID")

            total_election_ids = len(api_data)
            today = datetime.today()
            election_ids_starting_from_today = [
                record['election_id']
                for record in api_data
                if record.get("election_range_start_date") and datetime.strptime(
                    record.get("election_range_start_date"),
                    '%Y-%m-%d'
                ) >= today
            ]

            election_ids_before_today = [
                record['election_id']
                for record in api_data
                if record.get("election_range_start_date") and datetime.strptime(
                    record.get("election_range_start_date"),
                    '%Y-%m-%d'
                ) < today
            ]

            logger.info(f"Found Total of {total_election_ids} unique Election IDs")
            truncate_table(cursor, table_to_truncate, db_type)
            logger.info("Processing Election Guide JSON payload data")
            logger.info(
                f"Found {len(election_ids_starting_from_today)} Election IDs starting from today {today}"
            )
            logger.info(
                f"Found {len(election_ids_before_today)} Election IDs with Election Dates before today {today}"
            )
            logger.info(f"Inserting Total of {len(election_ids_starting_from_today)} unique Election IDs")
            raw_data = []
            for record in api_data:
                if record.get("election_range_start_date"):
                    record_start_date = datetime.strptime(record.get("election_range_start_date"), '%Y-%m-%d')
                    if record_start_date >= today:
                        insert_eguide_election_data(cursor, record, db_type)
                        total_inserted += 1
                        logger.info(
                            f"Inserting Election ID {record['election_id']} – {record['election_name']['en_US']} – {record_start_date.strftime('%Y-%m-%d')}"
                        )
                        raw_data.append(record)

            import json

            with open("raw_data.json", "w") as f:
                f.write(json.dumps(raw_data,indent=4))

            if db_type == "mssql":
                cursor.commit()
            logger.info("Election Guide Data Successfully Inserted")

        else:
            logger.info("Script Terminated")
            sys.exit(1)
    except Exception as ex:
        logger.error(f"An error occurred {ex}")
    finally:
        cursor.close()
        connection.close()
        logger.info(f"INFO Election Guide Data Successfully Inserted {total_inserted} Election Records")
        logger.info(f"Terminating Script – {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
