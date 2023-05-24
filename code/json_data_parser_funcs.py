import pandas as pd
from pathlib import Path

from typing import Dict
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

if __name__ == "__main__":
    json_file_path = Path().cwd().parent / "data" / "competitions.json"
    competitions_dict = read_json_file_to_dict(json_file_path)
    df_competitions = parse_and_process_competitions_dict(competitions_dict)
    print(tabulate(df_competitions, headers='keys', tablefmt='psql'))