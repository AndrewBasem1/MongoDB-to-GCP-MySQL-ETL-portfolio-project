import sqlalchemy
from helpful_funcs import read_json_file_to_dict
from pathlib import Path

def create_sql_db_connection(creds_file_path: Path=None) -> sqlalchemy.Connection:
    """
    This function creates a connection to a MySQL database using the credentials in the creds_file_path.
    
    ## Parameters:
    - creds_file_path: Path
        Path to the json file containing the credentials for the MySQL database.
        
    ## Returns:
    - sqlalchemy.connection
        A connection to the MySQL database.
    """
    if creds_file_path is None:
        creds_file_path = Path().cwd() / 'mysql_creds.json'
    connection_string = read_json_file_to_dict(creds_file_path)
    connection_engine = sqlalchemy.create_engine(url=connection_string['connection_string'])
    return connection_engine.connect()  


def run_sql_commands_from_file(sql_commands_file_path: Path, connection: sqlalchemy.Connection) -> None:
    """
    This function runs a SQL command on a MySQL database.
    
    ## Parameters:
    - sql_commands_file_path: Path
        The path to the file containing the SQL commands to run.
    - connection: sqlalchemy.Connection
        The connection to the MySQL database.
        
    ## Returns:
    - None
    """
    with open(sql_commands_file_path, 'r') as sql_commands_file:
        sql_commands = sql_commands_file.read().split(';')[:-1]
    sql_commands_count = len(sql_commands)
    print(f'found {sql_commands_count} sql commands in {sql_commands_file_path}')
    
    try:
        with connection.begin() as trans:
            for i, sql_command in enumerate(sql_commands):
                print(f'running command {i+1} of {sql_commands_count}')
                connection.execute(sqlalchemy.text(sql_command))
        print('commiting the transaction')
    except Exception as e:
        print(f'error running sql command: {e}')
        print('rolling back the transaction')
        connection.rollback()
    return None

if __name__ == '__main__':
    with create_sql_db_connection() as connection:
        sql_commands_file_path = Path().cwd() / 'database_schema_creation_commands.sql'
        run_sql_commands_from_file(sql_commands_file_path, connection)
        print('done, closing the database connection')