# This module was used to port the json files to mongodb.
# it includes some processing to be concat mutiple json files into a single one.
# the processing was mainly adding fields to each document in the json file, so they
# can fit logically into a single collection in mongodb.
#
# it's not actively used, but it's here for reference.


from helpful_funcs import read_json_file_to_dict
from pathlib import Path
from typing import List
import pymongo
import pymongo.database
import pymongo.collection
import pymongo.results


def connect_to_mongo_database(creds_fp:Path=None) -> pymongo.database.Database:
    from pymongo.mongo_client import MongoClient
    from pymongo.server_api import ServerApi
    if creds_fp is None:
        creds_fp = Path().cwd() / 'creds' / 'mongodb_creds.json'
    connection_string = read_json_file_to_dict(creds_fp)['connection_string']
    # Create a new client and connect to the server
    client = pymongo.MongoClient(connection_string, server_api=ServerApi('1'))
    db = client.statsbomb
    return db


def add_documents_to_collection(collection: pymongo.collection.Collection, documents: List[dict]) -> pymongo.results.InsertOneResult:
    return collection.insert_many(documents=documents)


def create_and_populate_competitions_collection(competitions_json_path: Path,
                                                competitions_collection: pymongo.collection.Collection
                                                ) -> pymongo.results.InsertManyResult:
    competitions_dict = read_json_file_to_dict(competitions_json_path)
    insert_many_result = competitions_collection.insert_many(documents=competitions_dict)
    if insert_many_result.acknowledged:
        print('The documents were successfully inserted into the competitions collection.')
        return insert_many_result
    else:
        raise Exception('The documents were not successfully inserted into the competitions collection.')


def create_and_populate_matches_collection(matches_dir:Path,
                                           matches_collection: pymongo.collection.Collection
                                           ) -> None:
    json_files = list(matches_dir.glob('**/*.json'))
    json_files_count = len(json_files)
    for i, match_json_path in enumerate(json_files):
        try:
            match_dict = read_json_file_to_dict(match_json_path)
            print(f'[{i}/{json_files_count}] - documents for competition {match_dict[0]["competition"]["competition_name"]} and season {match_dict[0]["season"]["season_name"]} starting insert.')
            insert_many_result = matches_collection.insert_many(documents=match_dict)
            if insert_many_result.acknowledged:
                print(f'[{i}/{json_files_count}] - documents for competition {match_dict[0]["competition"]["competition_name"]} and season {match_dict[0]["season"]["season_name"]} were successfully inserted into the matches collection.')
            else:
                raise Exception('The documents were not successfully inserted into the matches collection.')
        except Exception as e:
            print(f'***** ERROR ***** - {i}/{json_files_count} - {match_json_path}')
            print(e)
            continue


def create_and_populate_lineups_collection(lineups_dir:Path,
                                           lineups_collection: pymongo.collection.Collection
                                           ) -> None:
    json_files = list(lineups_dir.glob('**/*.json'))
    json_files_count = len(json_files)
    for i, lineup_json_path in enumerate(json_files):
        try:
            lineup_dict = read_json_file_to_dict(lineup_json_path)
            for doc in lineup_dict:
                doc['match_id'] = int(lineup_json_path.stem)
            insert_many_result = lineups_collection.insert_many(documents=lineup_dict)
            if insert_many_result.acknowledged:
                print(f'[{i}/{json_files_count}] - documents for match {lineup_dict[0]["match_id"]} were successfully inserted into the lineups collection.')
        except Exception as e:
            print(f'***** ERROR ***** - {i}/{json_files_count} - {lineup_json_path}')
            continue
      
        
def create_and_populate_events_collection(events_dir:Path,
                                           events_collection: pymongo.collection.Collection
                                           ) -> None:
    json_files = list(events_dir.glob('**/*.json'))
    json_files_count = len(json_files)
    for i, event_json_path in enumerate(json_files):
        try:
            event_dict = read_json_file_to_dict(event_json_path)
            for doc in event_dict:
                doc['match_id'] = int(event_json_path.stem)
            insert_many_result = events_collection.insert_many(documents=event_dict)
            if insert_many_result.acknowledged:
                print(f'[{i}/{json_files_count}] - documents for match {event_dict[0]["match_id"]} were successfully inserted into the events collection.')
        except Exception as e:
            print(f'***** ERROR ***** - {i}/{json_files_count} - {event_json_path}')
            continue



if __name__ == '__main__':

    db = connect_to_mongo_database()
    #############################################################################

    # competitions_collection = pymongo.collection.Collection(database=db, name='competitions')
    # competitions_json_path = Path().cwd().parent / 'data' / 'competitions.json'
    # print(create_and_populate_competitions_collection(competitions_json_path=competitions_json_path,
    #                                             competitions_collection=competitions_collection))
    
    #############################################################################
    
    # matches_collection = pymongo.collection.Collection(database=db, name='matches')
    # matches_dir = Path().cwd().parent / 'data' / 'matches'
    # create_and_populate_matches_collection(matches_dir=matches_dir,
    #                                          matches_collection=matches_collection)
    
    #############################################################################
    
    # lineups_collection = pymongo.collection.Collection(database=db, name='lineups')
    # lineups_dir = Path().cwd().parent / 'data' / 'lineups'
    # create_and_populate_lineups_collection(lineups_dir=lineups_dir,
    #                                        lineups_collection=lineups_collection)
    # print(db.lineups.count_documents({}))
   
    #############################################################################
   
    # events_collection = pymongo.collection.Collection(database=db, name='events')
    # events_dir = Path().cwd().parent / 'data' / 'events'
    # create_and_populate_events_collection(events_dir=events_dir,
    #                                        events_collection=events_collection)
    # print(db.events.count_documents({}))
    
    