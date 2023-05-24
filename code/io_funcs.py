import pandas as pd

from json import load as json_file_stream_to_dict
from json import JSONDecodeError
from pathlib import Path


def read_json_file_to_dict(file_path:Path) -> dict:
    """
    Reads a json file and returns a dictionary
    
    ## Parameters
    file_path: Path
        Path to the json file
    
    ## Returns
    dict
        Dictionary with the contents of the json file
        
    ## Raises
    FileNotFoundError
        If the file does not exist
    """
    if not(file_path.exists()):
        raise FileNotFoundError(f"File {file_path} does not exist.")
    with open(file_path, "r") as json_file:
        try:
            return json_file_stream_to_dict(json_file)
        except JSONDecodeError:
            raise JSONDecodeError(f"File {file_path} is not a valid json file.")