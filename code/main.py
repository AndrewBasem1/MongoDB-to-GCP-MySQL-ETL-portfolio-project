import pandas as pd
# remove the SettingWithCopyWarning
pd.options.mode.chained_assignment = None  # default='warn'
from pathlib import Path

from helpful_funcs import read_json_file_to_dict

from json_data_parser_funcs import parse_and_process_competitions_dict
from json_data_parser_funcs import parse_and_process_matches_dict
from json_data_parser_funcs import normalize_countries_field_df_competitions

from mongodb_downloader import connect_to_mongo_database
from mongodb_downloader import download_collection_to_list_of_dicts

from mysql_db_funcs import create_sql_db_connection
from mysql_db_funcs import run_sql_commands_from_file

def main():
    print('starting ETL script')
    
    print('connecting to mongodb database')
    mongodb_db =  connect_to_mongo_database()
    
    competitions_collection = mongodb_db.get_collection('competitions')
    matches_collection = mongodb_db.get_collection('matches')

    print('downloading collections from mongodb database')
    competitions_dict = download_collection_to_list_of_dicts(competitions_collection)
    matches_dict = download_collection_to_list_of_dicts(matches_collection)
    print('closing mongodb connection')
    mongodb_db.client.close()
        
    print('processing json data')
    competitions_dframes_dict = parse_and_process_competitions_dict(competitions_dict)
    matches_dframes_dict = parse_and_process_matches_dict(matches_dict)
    
    competitions_dframes_dict['df_competitions'] = normalize_countries_field_df_competitions(competitions_dframes_dict['df_competitions'],
                                                                                             matches_dframes_dict['df_countries'])
    
    matches_dframes_dict['df_stadiums'].to_csv('testing.csv', index=False)
        
    print('connecting to mysql database')
    with create_sql_db_connection() as mysql_connection:
        mysql_connection.begin()
        print('reading database schema creation commands from file')
        sql_database_config_commands_file_path = Path().cwd() / 'sql_files' / 'database_schema_creation_commands.sql'
        run_sql_commands_from_file(sql_commands_file_path=sql_database_config_commands_file_path,
                                connection=mysql_connection)
        print('starting upload of data to mysql database')
    
        for table_name, dframe in competitions_dframes_dict.items():
            print(f'uploading table {table_name}')
            sql_table_name = table_name.replace('df_', '')
            dframe = dframe.dropna(axis=1, how='all')
            dframe.to_sql(name=sql_table_name,
                          con=mysql_connection,
                          if_exists='append',
                          index=False,
                          chunksize=10_000)
            print(f'done uploading table {table_name}')

        for table_name, dframe in matches_dframes_dict.items():
            print(f'uploading table {table_name}')
            sql_table_name = table_name.replace('df_', '')
            dframe = dframe.dropna(axis=1, how='all')
            dframe.to_sql(name=sql_table_name,
                          con=mysql_connection,
                          if_exists='append',
                          index=False,
                          chunksize=10_000)
            print(f'done uploading table {table_name}')
            
        print('creating foreign key constraints')
        sql_foreign_key_constraints_commands_fp = Path().cwd() / 'sql_files' / 'database_foreign_keys.sql'
        run_sql_commands_from_file(sql_foreign_key_constraints_commands_fp, mysql_connection)
    print('done all ETL steps')

if __name__ == '__main__':
    main()