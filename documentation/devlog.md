1. searched for a JSON/MongoDB dataset that includes nested datasets.
   
2. found a dataset produced by ['Statsbomb company'](https://statsbomb.com) related to football (soccer) matches.
   
3. downloaded the dataset, explored a bit till I understood its raw structure (refer to `documentation/data_guide.md` for more details).
   
4. designed a SQL database schema that can be used to store the data from the (competitions & matches) JSON files, while considering normalization and data integrity.
   
5. Wrote a proof of concept python script that reads the JSON files and transforms them into pandas dataframes that mimic the structure of the SQL database tables.
   
6. Migrated the data from the JSON files to a MongoDB database. (refer to `documentation/mongodb_data.md` for more details).
   
7. Altered the python script to read the data from the MongoDB database instead of the local JSON files.
   
8. Created a GCP account and hosted a mySQL database on it.

9. wrote the SQL commands that will create the barebones of the database (tables, primary keys, foreign keys, etc.)

10. wrote the python script that will upload the data to the mySQL database.

11. wrote the documentation for the project (refer to `documentation/` for more details).