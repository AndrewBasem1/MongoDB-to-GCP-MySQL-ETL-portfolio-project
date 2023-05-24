import pandas as pd
from pathlib import Path

from typing import Dict, List
from tabulate import tabulate
from helpful_funcs import read_json_file_to_dict
from helpful_funcs import downcast_all_numerical_cols_in_df


def parse_and_process_competitions_dict(competitions_dict:dict) -> Dict[str, pd.DataFrame]:
    """
    Parses and processes the competitions dictionary,
    and returns a dictionary with the competitions and seasons dataframes,
    these dataframes are normalized and ready to be saved to a SQL database.
    
    ## Parameters
    competitions_dict: dict
        Dictionary with the competitions data -> as read from the competitions.json file
    
    ## Returns
    dict
        Dictionary with the competitions and seasons dataframes
    """
    
    df_competitions = pd.DataFrame(competitions_dict)
    
    # dropping unnecessary columns (after visually inspecting the input data)
    for col in df_competitions.columns:
        if col.startswith('match'):
            df_competitions.drop(col, axis=1, inplace=True)
    
    # extracting the seasons table
    df_seasons = df_competitions[['competition_id', 'season_id', 'season_name']].drop_duplicates(ignore_index=True)
    df_seasons.season_name = df_seasons.season_name.str.split('/')
    ## extracting the start and end year of the season
    df_seasons['season_start_year'] = df_seasons.season_name.apply(lambda x: x[0]).astype(int)
    df_seasons['season_end_year'] = df_seasons.season_name.apply(lambda x: x[1] if len(x) > 1 else x[0]).astype(int)
    df_seasons.drop('season_name', axis=1, inplace=True)
    ## downcasting the numerical columns
    df_seasons = downcast_all_numerical_cols_in_df(df_seasons)
    
    # dropping unnecessary columns in the competitions table to normalize the data
    df_competitions.drop(['season_id', 'season_name'], axis=1, inplace=True)
    df_competitions.drop_duplicates(ignore_index=True, inplace=True)
    df_competitions = downcast_all_numerical_cols_in_df(df_competitions)
    
    dict_return = {'competitions_df': df_competitions, 'seasons_df': df_seasons}
    return dict_return


def get_competition_season_matches_json_file_paths(matches_dir_path:Path=None) -> List[Path]:
    """
    Due to the way the data is structured, the matches data is stored in a directory structure.
    Where each competition has a directory, and each season has a directory inside the competition directory.
    And each match is stored as a json file inside the season directory.
    
    We need to get the paths to all the json files, so we only iterate while reading, not while parsing.
    
    ## Parameters
    matches_dir_path: Path, default=None
        Path to the directory where the matches json files are stored. (default is the matches directory in the data directory)
    
    ## Returns
    List[Path]
        List of paths to the matches json files
    """
    if matches_dir_path is None:
        matches_dir_path = Path().cwd().parent / "data" / "matches"
    if not(matches_dir_path.exists() and matches_dir_path.is_dir()):
        raise FileNotFoundError(f"Directory {matches_dir_path} does not exist.")
    return list(matches_dir_path.glob(pattern='**/*.json'))

def read_matches_dicts_from_json_files(matches_json_file_paths:List[Path]) -> List[dict]:
    """
    Reads the matches json files into a list of dictionaries.
    
    ## Parameters
    matches_json_file_paths: List[Path]
        List of paths to the matches json files
    
    ## Returns
    List[dict]
        List of dictionaries with the matches data
    """
    matches_dicts = []
    for json_file_path in matches_json_file_paths:
        matches_dicts.extend(read_json_file_to_dict(json_file_path))
    return matches_dicts

def parse_and_process_matches_dict(matches_dict:dict) -> pd.DataFrame:
    df_matches = pd.json_normalize(matches_dict)
    
    df_matches['match_datetime'] = (df_matches.match_date) + ' ' + (df_matches.kick_off)
    df_matches.match_datetime = pd.to_datetime(df_matches.match_datetime, format='%Y-%m-%d %H:%M:%S')
    
    df_matches.drop(columns=['match_date', 'kick_off'], inplace=True)
    
    
    dataframes_dict = {}
    dataframes_dict['df_matches'] = df_matches
    # dropping unnecessary columns (after visually inspecting the input data)
    cols_to_drop = ['match_status',
                    'match_status_360',
                    'last_updated',
                    'last_updated_360',
                    'competition.country_name',
                    'competition.competition_name',
                    'season.season_name',
                    'metadata.data_version',
                    'metadata.shot_fidelity_version',
                    'metadata.xy_fidelity_version']
    cols_to_rename = {'competition.competition_id': 'competition_id',
                      'season.season_id': 'season_id'}

    df_matches.drop(cols_to_drop, axis=1, inplace=True)
    df_matches.rename(columns=cols_to_rename, inplace=True)
    
    
    def extract_sub_df(cols_to_extract:Dict[str,str], dataframe_name:str) -> None:
        """
        Extracts a sub dataframe from the main matches dataframe, and adds it to the dataframes dictionary.
        
        ## Parameters
        cols_to_extract: Dict[str,str]
            Dictionary with the columns to extract from the main matches dataframe, and the new names of these columns
        dataframe_name: str
            Name of the new dataframe
        """
        df = df_matches[list(cols_to_extract.keys())].drop_duplicates(ignore_index=True)
        df.rename(columns=cols_to_extract, inplace=True)
        df = downcast_all_numerical_cols_in_df(df)
        dataframes_dict[dataframe_name] = df
        df_matches.drop(list(cols_to_extract.keys()), axis=1, inplace=True)
        return None
    
    # creating competiton stages table
    competitions_stages_cols={'competition_stage.id': 'competition_stage_id',
                              'competition_stage.name': 'competition_stage_name'}
    dataframe_name = 'df_competition_stages'
    extract_sub_df(competitions_stages_cols, dataframe_name)
    
    
    # NOTE: all of the upcoming dataframes will have a (country name <> country id) pair, I'll parse all of them into each table
    # and then finally prepare the countries table
    
    # creating stadiums table
    stadiums_cols = {'stadium.id': 'stadium_id',
                     'stadium.name': 'stadium_name',
                     'stadium.country.id': 'country_id',
                     'stadium.country.name': 'country_name'}
    dataframe_name = 'df_stadiums'
    extract_sub_df(stadiums_cols, dataframe_name)
    
    
    # creating referees table
    referees_cols = {'referee.id': 'referee_id',
                     'referee.name': 'referee_name',
                     'referee.country.id': 'country_id',
                     'referee.country.name': 'country_name'}
    dataframe_name = 'df_referees'
    extract_sub_df(referees_cols, dataframe_name)
    
    def parse_teams_and_managers_data(is_home_team:bool) -> Dict[str, pd.DataFrame]:
        prefix = 'home_team' if is_home_team else 'away_team'
        cols_to_extract = {'match_id': 'match_id',
                           f'{prefix}.{prefix}_id': 'team_id',
                           f'{prefix}.{prefix}_name': 'team_name',
                           f'{prefix}.{prefix}_gender': 'team_gender',
                           f'{prefix}.{prefix}_group': 'team_group',
                           f'{prefix}.country.id': 'country_id',
                           f'{prefix}.country.name': 'country_name',
                           f'{prefix}.managers': 'managers'}
        
        df_team_step_1 = df_matches[cols_to_extract.keys()]
        df_team_step_1.rename(columns=cols_to_extract, inplace=True)
        
        base_team_info_cols = ['team_id', 'team_name', 'team_gender', 'country_id', 'country_name']
        df_team_base_info = df_team_step_1[base_team_info_cols].drop_duplicates(ignore_index=True)
        
        df_team_managers_step_1 = df_team_step_1[['match_id', 'team_id', 'managers']]
        df_team_managers_step_2 = df_team_managers_step_1.explode('managers').reset_index(drop=True)
        
        df_team_managers_step_3_left = df_team_managers_step_2.drop(columns=['managers'])
        df_team_managers_step_3_right = pd.json_normalize(df_team_managers_step_2.managers)
        df_team_managers_step_3_right.rename(columns={'id': 'manager_id',
                                                      'name': 'manager_name',
                                                      'nickname': 'manager_nickname',
                                                      'dob': 'manager_dob',
                                                      'country.id': 'country_id',
                                                      'country.name': 'country_name'}, 
                                             inplace=True)
        df_team_managers_step_3_right.manager_dob = pd.to_datetime(df_team_managers_step_3_right.manager_dob, format='%Y-%m-%d')
        
        df_team_managers_step_4 = pd.concat([df_team_managers_step_3_left,df_team_managers_step_3_right], axis=1)
        
        df_team_managers_matches = df_team_managers_step_4[['match_id', 'team_id', 'manager_id']].drop_duplicates(ignore_index=True)
                
        df_managers_base_data = df_team_managers_step_4[['manager_id',
                                                         'manager_name',
                                                         'manager_nickname',
                                                         'manager_dob',
                                                         'country_id',
                                                         'country_name']].drop_duplicates(ignore_index=True)
        
        
        dict_return = {'df_team_base_info': df_team_base_info,
                       'df_team_managers_matches': df_team_managers_matches,
                       'df_managers_base_data': df_managers_base_data}
        
        cols_to_extract.pop('match_id')
        cols_to_extract.pop(f'{prefix}.{prefix}_id')
        
        df_matches.drop(list(cols_to_extract.keys()), axis=1, inplace=True)
        
        df_matches.rename(columns={f'{prefix}.{prefix}_id': f'{prefix}_id'}, inplace=True)
        
        return dict_return
    
    # parsing home team data
    dict_home_team_data = parse_teams_and_managers_data(is_home_team=True)
    dict_away_team_data = parse_teams_and_managers_data(is_home_team=False)
    
    df_team_base_info = pd.concat([dict_home_team_data['df_team_base_info'], dict_away_team_data['df_team_base_info']], axis=0, ignore_index=True)
    df_team_managers_matches = pd.concat([dict_home_team_data['df_team_managers_matches'], dict_away_team_data['df_team_managers_matches']], axis=0, ignore_index=True)
    df_managers_base_data = pd.concat([dict_home_team_data['df_managers_base_data'], dict_away_team_data['df_managers_base_data']], axis=0, ignore_index=True)
    
    dataframes_dict['df_team_base_info'] = df_team_base_info.drop_duplicates(ignore_index=True)
    dataframes_dict['df_team_managers_matches'] = df_team_managers_matches.drop_duplicates(ignore_index=True)
    dataframes_dict['df_managers_base_data'] = df_managers_base_data.drop_duplicates(ignore_index=True)
          
    return dataframes_dict

if __name__ == "__main__":
    json_file_path = Path().cwd().parent / "data" / "competitions.json"
    competitions_dict = read_json_file_to_dict(json_file_path)
    df_competitions = parse_and_process_competitions_dict(competitions_dict)
    print(tabulate(df_competitions, headers='keys', tablefmt='psql'))