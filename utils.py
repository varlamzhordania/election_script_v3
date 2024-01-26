import requests
import pyodbc
from logger import main_logger as logger
import mysql.connector
import configparser
from datetime import datetime, timedelta

config = configparser.ConfigParser()
config.read('config.ini')


def connect_to_sql_server(server, database, username, password):
    """
    Connect to Microsoft SQL Server using provided parameters.

    :param server: SQL Server hostname or IP address.
    :param database: Name of the database to connect to.
    :param username: Database username for authentication.
    :param password: Password associated with the provided username.
    :return: Cursor object for executing SQL queries.
    """
    # Construct the connection string
    connection_string = (
        f'DRIVER={{SQL Server}};'
        f'SERVER={server};'
        f'DATABASE={database};'
        f'UID={username};'
        f'PWD={password};'
    )

    try:
        connection = pyodbc.connect(connection_string)
        cursor = connection.cursor()
        return cursor, connection

    except:
        logger.error(f"Error connecting to SQL Server:")
        return None


def connect_to_mysql_server(host, database, user, password):
    try:
        connection = mysql.connector.connect(
            host=host,
            database=database,
            user=user,
            password=password
        )
        if connection.is_connected():
            cursor = connection.cursor()
            return cursor, connection
        else:
            return None, None
    except mysql.connector.Error as ex:
        logger.error(f"Error connecting to MySQL Server: {ex}")
        return None, None


def truncate_table(cursor, table_name, db_type):
    """
    Truncate a table in the database.

    :param cursor: Database cursor.
    :param table_name: Name of the table to be truncated.
    """
    try:
        logger.info(f"Purging (Truncate) Election Guide DB Table")

        # Get the current row count before truncating
        cursor.execute(f'SELECT COUNT(*) FROM {table_name}')
        row_count_before = cursor.fetchone()[0]

        # Construct the SQL query to truncate the table
        sql_query = f'TRUNCATE TABLE {table_name};'

        # Execute the query
        cursor.execute(sql_query)

        # Commit the transaction
        if db_type == "mssql":
            cursor.commit()

        # Get the row count after truncating
        cursor.execute(f'SELECT COUNT(*) FROM {table_name}')
        row_count_after = cursor.fetchone()[0]

        rows_removed = row_count_before - row_count_after
        logger.info(f"Purged (Removed) {rows_removed} Rows from Election Guide DB Table")

    except Exception as ex:
        # Handle SQL errors
        logger.error(f"Error truncating table {table_name}: {ex}")


def create_table_if_not_exists_mysql(cursor):
    # Define the table creation script for MySQL
    table_creation_script_mysql = """
    CREATE TABLE IF NOT EXISTS ElectionGuide (
        ID INT AUTO_INCREMENT PRIMARY KEY,
        ExternalID VARCHAR(1024),
        Type VARCHAR(128),
        NameEncode VARCHAR(1024),
        Geometry GEOMETRY,
        Location TEXT,
        StartTime DATETIME,
        EndTime DATETIME,
        Description TEXT,
        Url VARCHAR(2048),
        ImageUrl VARCHAR(2048),
        CreatedTime DATETIME,
        UpdatedTime DATETIME,
        Source TEXT,
        ElectionNameEncode TEXT,
        Encode TEXT,
        ElectIssues TEXT,
        Snap TEXT,
        OrigElectYear TEXT,
        CovidDelay TEXT,
        CovidEffects TEXT,
        ElectStartDate DATETIME,
        ElectEndDate DATETIME,
        ElectBlackoutStartDate DATETIME,
        ElectBlackoutEndDate DATETIME,
        Category TEXT,
        SubCategory TEXT,
        ElecSys TEXT,
        ElectCommName TEXT,
        DistrictID TEXT,
        Title TEXT,
        CountryCode TEXT,
        DistrictType TEXT,
        PubDate TEXT,
        GovFun TEXT,
        GovFunUpdate TEXT,
        RegDeadline TEXT,
        VotingAge TEXT,
        EligibleVoters TEXT,
        FirstTimeVoters TEXT,
        VotingType TEXT,
        VotingPrimary TEXT,
        VotingStartDate DATETIME,
        VotingEndDate DATETIME,
        Excuse TEXT,
        DaysOffset TEXT,
        ElectName TEXT
    );
    """
    try:
        # Check if the table exists
        cursor.execute("SHOW TABLES LIKE 'ElectionGuide'")
        if cursor.fetchone():
            logger.info("Table 'ElectionGuide' already exists.")
        else:
            # Create the table if it doesn't exist
            cursor.execute(table_creation_script_mysql)
            logger.info("Table 'ElectionGuide' created successfully.")
    except Exception as e:
        logger.error(f"Error: {e}")


def create_table_if_not_exists_sql_server(cursor):
    # Define the table creation script for SQL Server
    table_creation_script_sql_server = """
    CREATE TABLE dbo.ElectionGuide (
        ID INT IDENTITY(1,1) PRIMARY KEY,
        ExternalID NVARCHAR(1024),
        Type NVARCHAR(128),
        NameEncode NVARCHAR(1024),
        Geometry GEOMETRY,
        Location NVARCHAR(MAX),
        StartTime DATETIMEOFFSET(7),
        EndTime DATETIMEOFFSET(7),
        Description NVARCHAR(MAX),
        Url NVARCHAR(2048),
        ImageUrl NVARCHAR(2048),
        CreatedTime DATETIMEOFFSET(7),
        UpdatedTime DATETIMEOFFSET(7),
        Source NVARCHAR(MAX),
        ElectionNameEncode NVARCHAR(MAX),
        Encode NVARCHAR(MAX),
        ElectIssues NVARCHAR(MAX),
        Snap NVARCHAR(MAX),
        OrigElectYear NVARCHAR(MAX),
        CovidDelay NVARCHAR(MAX),
        CovidEffects NVARCHAR(MAX),
        ElectStartDate DATETIMEOFFSET(7),
        ElectEndDate DATETIMEOFFSET(7),
        ElectBlackoutStartDate DATETIMEOFFSET(7),
        ElectBlackoutEndDate DATETIMEOFFSET(7),
        Category NVARCHAR(MAX),
        SubCategory NVARCHAR(MAX),
        ElecSys NVARCHAR(MAX),
        ElectCommName NVARCHAR(MAX),
        DistrictID NVARCHAR(MAX),
        Title NVARCHAR(MAX),
        CountryCode NVARCHAR(MAX),
        DistrictType NVARCHAR(MAX),
        PubDate NVARCHAR(MAX),
        GovFun NVARCHAR(MAX),
        GovFunUpdate NVARCHAR(MAX),
        RegDeadline NVARCHAR(MAX),
        VotingAge NVARCHAR(MAX),
        EligibleVoters NVARCHAR(MAX),
        FirstTimeVoters NVARCHAR(MAX),
        VotingType NVARCHAR(MAX),
        VotingPrimary NVARCHAR(MAX),
        VotingStartDate DATETIMEOFFSET(7),
        VotingEndDate DATETIMEOFFSET(7),
        Excuse NVARCHAR(MAX),
        DaysOffset NVARCHAR(MAX),
        ElectName NVARCHAR(MAX)
    );
    """
    try:
        # Check if the table exists
        cursor.execute("SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'ElectionGuide'")
        if cursor.fetchone():
            logger.info("Table 'ElectionGuide' already exists.")
        else:
            # Create the table if it doesn't exist
            cursor.execute(table_creation_script_sql_server)
            cursor.commit()
            logger.info("Table 'ElectionGuide' created successfully.")
    except Exception as e:
        logger.error(f"Error: {e}")


def get_api_data(api_url, token):
    """
    Fetch data from an API endpoint using a provided token.

    :param api_url: URL of the API endpoint.
    :param token: Authorization token for accessing the API.

    :return: Parsed JSON data from the API if the response status code is 200.
            None if there's an error or the status code is not 200.
    """
    try:
        # Make the API request with the provided token
        response = requests.get(
            api_url, headers={"Authorization": f"Token {token}"}
        )

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            # Parse and return the JSON data
            data = response.json()
            return data
        else:
            # Handle non-200 status codes
            logger.error("API returned error or invalid data")
            logger.error(f"Error: Unable to fetch data from the API. Status code: {response.status_code}")
            return None

    except requests.RequestException as ex:
        # Handle general request exceptions
        logger.error("API returned error or invalid data")
        logger.error(f"Error making API request: {ex}")
        return None


def insert_eguide_election_data(cursor, data, cursor_type="mssql"):
    election_id = data['election_id']

    voting_methods_type = None
    voting_methods_primary = None
    voting_methods_start_date = None
    voting_methods_end_date = None
    voting_methods_execuse_required = None
    voting_methods_instructions = None

    # Check if data['voting_methods'] is not None
    if data.get('voting_methods') is not None:
        # Update values if voting_methods is present in data
        voting_methods_type = '!!'.join(str(method.get('type', '')) for method in data['voting_methods'])
        voting_methods_primary = '!!'.join(str(method.get('primary', '')) for method in data['voting_methods'])
        voting_methods_start_date = '!!'.join(str(method.get('start', '')) for method in data['voting_methods'])
        voting_methods_end_date = '!!'.join(str(method.get('end', '')) for method in data['voting_methods'])
        voting_methods_execuse_required = '!!'.join(
            str(method.get('excuse-required', '')) if method.get('excuse-required') is not None else '' for method in
            data['voting_methods']
        )
        voting_methods_instructions = '!!'.join(
            str(method.get('instructions', '')) for method in data['voting_methods']
        )

    # Extracting specific fields from the JSON data
    election_data = {
        'election_id': election_id,
        'election_name_encode': 'en_US',
        'election_name': data['election_name']['en_US'],
        'election_date_updated': data.get('date_updated', ''),
        'election_issues': data.get('election_issues', ''),
        'is_snap_election': data.get('is_snap_election', ''),
        'original_election_year': data.get('original_election_year', ''),
        'election_range_start_date': data.get('election_range_start_date', ''),
        'election_range_end_date': data.get('election_range_end_date', ''),
        'is_delayed_covid19': str(data.get('is_delayed_covid19', '')),
        'covid_effects': data.get('covid_effects', ''),
        'election_declared_start_date': data.get('election_declared_start_date', ''),
        'election_declared_end_date': data.get('election_declared_end_date', ''),
        'election_blackout_start_date': data.get('election_blackout_start_date', ''),
        'election_blackout_end_date': data.get('election_blackout_end_date', ''),
        'election_type': data.get('election_type', ''),
        'election_scope': data.get('election_scope', ''),
        'electoral_system': data.get('electoral_system', ''),
        'election_commission_name': data.get('election_commission_name', ''),
        'administring_election_commission_website': data.get('administering_election_commission_website', ''),
        'election_source': data.get('source', ''),
        'district_ocd_id': data['district'].get("district_ocd_id"),
        'district_name': data['district'].get("district_name"),
        'district_country': data['district'].get("district_country"),
        'district_type': data['district'].get("district_type"),
        'government_functions': data['government_functions'].get('details', ''),
        'government_functions_updated_date': data['government_functions'].get('updated', ''),
        'voter_registration_day_deadline': data.get('voter_registration_day', ''),
        'voting_age_minimum_inclusive': data.get('voting_age_minimum_inclusive', ''),
        'eligible_voters': data.get('eligible_voters', None),
        'first_time_voters': data.get('first_time_voters', None),
        'voting_methods_type': voting_methods_type,
        'voting_methods_primary': voting_methods_primary,
        'voting_methods_start_date': voting_methods_start_date,
        'voting_methods_end_date': voting_methods_end_date,
        'voting_methods_execuse_required': voting_methods_execuse_required,
        'voting_methods_instructions': voting_methods_instructions,
    }
    if election_data['election_declared_start_date'] is None:
        if election_data['election_range_start_date']:
            election_data['election_declared_start_date'] = election_data['election_range_start_date']
        else:
            election_data['election_range_start_date'] = election_data['voting_methods_start_date']
            election_data['election_declared_start_date'] = election_data['voting_methods_start_date']

    elif election_data['election_range_start_date'] is None:
        election_data['election_range_start_date'] = election_data['election_declared_start_date']

    elif election_data['voting_methods_start_date'] is None:
        election_data['voting_methods_start_date'] = election_data['election_declared_start_date']

    if not election_data['government_functions']:
        election_data['government_functions'] = None

    if not election_data['voting_methods_execuse_required']:
        election_data['voting_methods_execuse_required'] = None

    election_start_date_str = election_data.get("election_declared_start_date")
    election_start_date = datetime.strptime(election_start_date_str, '%Y-%m-%d')
    now = datetime.now()
    days_offset = election_start_date - now + timedelta(days=1)

    if days_offset.days < 0:
        election_data['days_offset'] = 1
    else:
        election_data['days_offset'] = days_offset.days

    # Inserting data into the ElectionGuide table
    if cursor_type == "mssql":
        cursor.execute(
            '''
            INSERT INTO dbo.ElectionGuide (
                ExternalID,
                NameEncode,
                ElectName,
                PubDate,
                ElectIssues,
                Snap,
                OrigElectYear,
                StartTime,
                EndTime,
                CovidDelay,
                CovidEffects,
                ElectStartDate,
                ElectEndDate,
                ElectBlackoutStartDate,
                ElectBlackoutEndDate,
                Category,
                SubCategory,
                ElecSys,
                ElectCommName,
                Url,
                Source,
                DistrictID,
                Title,
                CountryCode,
                DistrictType,
                GovFun,
                GovFunUpdate,
                RegDeadline,
                VotingAge,
                EligibleVoters,
                FirstTimeVoters,
                VotingType,
                VotingPrimary,
                VotingStartDate,
                VotingEndDate,
                Excuse,
                Description,
                DaysOffset
            )
            VALUES (
                ?,
                ?,
                ?,
                ?,
                ?,
                ?,
                ?,
                TRY_CONVERT(datetimeoffset, ?, 127),
                TRY_CONVERT(datetimeoffset, ?, 127),
                ?,
                ?,
                TRY_CONVERT(datetimeoffset, ?, 127),
                TRY_CONVERT(datetimeoffset, ?, 127),
                TRY_CONVERT(datetimeoffset, ?, 127),
                TRY_CONVERT(datetimeoffset, ?, 127),
                ?,
                ?,
                ?,
                ?,
                ?,
                ?,
                ?,
                ?,
                ?,
                ?,
                ?,
                ?,
                ?,
                ?,
                ?,
                ?,
                ?,
                ?,
                TRY_CONVERT(datetimeoffset, ?, 127),
                TRY_CONVERT(datetimeoffset, ?, 127),
                ?,
                ?,
                ?
            )
            ''', (
                election_data['election_id'],
                election_data['election_name_encode'],
                election_data['election_name'],
                election_data['election_date_updated'],
                election_data['election_issues'],
                election_data['is_snap_election'],
                election_data['original_election_year'],
                election_data['election_range_start_date'],
                election_data['election_range_end_date'],
                election_data['is_delayed_covid19'],
                election_data['covid_effects'],
                election_data['election_declared_start_date'],
                election_data['election_declared_end_date'],
                election_data['election_blackout_start_date'],
                election_data['election_blackout_end_date'],
                election_data['election_type'],
                election_data['election_scope'],
                election_data['electoral_system'],
                election_data['election_commission_name'],
                election_data['administring_election_commission_website'],
                election_data['election_source'],
                election_data['district_ocd_id'],
                election_data['district_name'],
                election_data['district_country'],
                election_data['district_type'],
                election_data['government_functions'],
                election_data['government_functions_updated_date'],
                election_data['voter_registration_day_deadline'],
                election_data['voting_age_minimum_inclusive'],
                election_data['eligible_voters'],
                election_data['first_time_voters'],
                election_data['voting_methods_type'],
                election_data['voting_methods_primary'],
                election_data['voting_methods_start_date'],
                election_data['voting_methods_end_date'],
                election_data['voting_methods_execuse_required'],
                election_data['voting_methods_instructions'],
                election_data['days_offset']
            )
        )

    elif cursor_type == "mysql":
        cursor.execute(
            '''
            INSERT INTO ElectionGuide (
                ExternalID,
                NameEncode,
                ElectName,
                PubDate,
                ElectIssues,
                Snap,
                OrigElectYear,
                StartTime,
                EndTime,
                CovidDelay,
                CovidEffects,
                ElectStartDate,
                ElectEndDate,
                ElectBlackoutStartDate,
                ElectBlackoutEndDate,
                Category,
                SubCategory,
                ElecSys,
                ElectCommName,
                Url,
                Source,
                DistrictID,
                Title,
                CountryCode,
                DistrictType,
                GovFun,
                GovFunUpdate,
                RegDeadline,
                VotingAge,
                EligibleVoters,
                FirstTimeVoters,
                VotingType,
                VotingPrimary,
                VotingStartDate,
                VotingEndDate,
                Excuse,
                Description,
                DaysOffset
            )
            VALUES (
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s
            )
            ''', (
                election_data['election_id'],
                election_data['election_name_encode'],
                election_data['election_name'],
                election_data['election_date_updated'],
                election_data['election_issues'],
                election_data['is_snap_election'],
                election_data['original_election_year'],
                election_data['election_range_start_date'],
                election_data['election_range_end_date'],
                election_data['is_delayed_covid19'],
                election_data['covid_effects'],
                election_data['election_declared_start_date'],
                election_data['election_declared_end_date'],
                election_data['election_blackout_start_date'],
                election_data['election_blackout_end_date'],
                election_data['election_type'],
                election_data['election_scope'],
                election_data['electoral_system'],
                election_data['election_commission_name'],
                election_data['administring_election_commission_website'],
                election_data['election_source'],
                election_data['district_ocd_id'],
                election_data['district_name'],
                election_data['district_country'],
                election_data['district_type'],
                election_data['government_functions'],
                election_data['government_functions_updated_date'],
                election_data['voter_registration_day_deadline'],
                election_data['voting_age_minimum_inclusive'],
                election_data['eligible_voters'],
                election_data['first_time_voters'],
                election_data['voting_methods_type'],
                election_data['voting_methods_primary'],
                election_data['voting_methods_start_date'],
                election_data['voting_methods_end_date'],
                election_data['voting_methods_execuse_required'],
                election_data['voting_methods_instructions'],
                election_data['days_offset']
            )
        )
    else:
        logger.error("Unsupported database type, ", cursor_type)
        return None
