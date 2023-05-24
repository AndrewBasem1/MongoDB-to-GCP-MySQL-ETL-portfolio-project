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
    
    # creating competiton stages table
    competitions_stages_cols = ['competition_stage.id','competition_stage.name']
    df_competition_stages = df_matches[competitions_stages_cols].drop_duplicates(ignore_index=True)
    df_competition_stages.rename(columns={'competition_stage.id': 'competition_stage_id',
                                          'competition_stage.name': 'competition_stage_name'},
                                 inplace=True)
    df_competition_stages = downcast_all_numerical_cols_in_df(df_competition_stages)
    df_matches.drop(columns=competitions_stages_cols, inplace=True)
    
    #
    return df_matches

if __name__ == "__main__":
    json_file_path = Path().cwd().parent / "data" / "competitions.json"
    competitions_dict = read_json_file_to_dict(json_file_path)
    df_competitions = parse_and_process_competitions_dict(competitions_dict)
    print(tabulate(df_competitions, headers='keys', tablefmt='psql'))