import pandas as pd
import pymongo
import pymongo.database
import pymongo.collection
import pymongo.results
import pymongo.server_api

from json import loads as json_loads
from pathlib import Path
from helpful_funcs import read_json_file_to_dict
from tabulate import tabulate
from typing import List

def connect_to_mongo_database(creds_json_file_path:Path=None) -> pymongo.database.Database:
    """
    connects to the mongodb database and returns the database object
    this can then be used to query different collections in the mongodb database
    
    ## Parameters
    creds_json_file_path: Path
        Path to the json file with the credentials to connect to the mongodb database
        (not sharing them publicly on the repo for obvious reasons...)
        
    ## Returns
    pymongo.database.Database
        Database object to query the mongodb database
    """
    # the credentials are stored in a json file
    if creds_json_file_path is None:
        creds_json_path = Path().cwd() / 'mongodb_creds.json'
    creds_dict = read_json_file_to_dict(creds_json_path)
    username = creds_dict['username']
    password = creds_dict['password']
    uri = f'mongodb+srv://{username}:{password}@statsbomb.znogzta.mongodb.net/?retryWrites=true&w=majority'
    # Create a new client and connect to the server
    client = pymongo.MongoClient(uri, server_api=pymongo.server_api.ServerApi('1'))
    db = client.statsbomb
    return db

def download_collection_to_list_of_dicts(collection:pymongo.collection.Collection) -> List[dict]:
    """
    This function downloads a collection from the mongodb database and returns it as a list of dictionaries.
    which can then be processed into a pandas dataframe.
    
    ## Parameters
    collection: pymongo.collection.Collection
        Collection to download from the mongodb database
    
    ## Returns
    List[dict]
        List of dictionaries with the contents of the collection
    """
    docs_cursor = collection.find({}, {'_id': False})       
    return list(docs_cursor)

if __name__ == '__main__':
    db = connect_to_mongo_database()
    competitions_collection = db.competitions
    docs_list = download_collection_to_list_of_dicts(competitions_collection)
    print(type(docs_list[0]))