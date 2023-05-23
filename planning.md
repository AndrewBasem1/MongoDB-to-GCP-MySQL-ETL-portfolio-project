# **Planning**

## **1. Introduction**

I'll start the project by defining milestones that build on each other, where each milestone constitutes a working product.

## **2. Milestones**

### **2.1. Milestone 1**

- #### `End goal:`
    - a local working ETL process using Python to transform nested JSON data into seperate pandas dataframes that can be then loaded into a PostgreSQL database.
  
- #### `Deliverables:`
    1. A JSON file containing the raw data (this will resemble the data -collection- that is stored in the MongoDB database), preferably having a nested structure. (or a public API that can give me access to live JSON data)
    2. A Python script (or Jupyter notebook) that extracts data from the JSON files and transforms it into pandas dataframes. these dataframes should mimic the structure of the tables in a relational SQL database. (having primary and foreign keys, etc.)
    3. The python script should also include creation of new fields that are derived from the raw data.

- #### `Actions to do:`
    1. Search for a dataset that has a nested JSON structure. (preferably a dataset that is related to an events database)
    2. Investigate the structure of the dataset and define the tables that will be created in the SQL database.
    3. Write the Python script that extracts and transforms the data into pandas dataframes.

- #### `Timeframe:`
    - 1<sup>st</sup> day.

- #### `Status:`
    - In progress.