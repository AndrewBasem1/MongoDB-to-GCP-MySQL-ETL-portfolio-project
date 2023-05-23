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

### 2.2. Milestone 2

- #### `End goal:`
    - Hosting the JSON data on a MongoDB database and connecting to it using Python.

- #### `Deliverables:`
    1. A MongoDB database containing the JSON data.
    2. Updating the python function that reads the local JSON file to connect to the MongoDB database, and read the data from there.

- #### `Actions to do:`
    1. Brush up on using MongoDB.
    2. All items in the `Deliverables` section of Milestone 2.

- #### `Timeframe:`
    - 2<sup>nd</sup> day.

- #### `Status:`
    - Not started.

### 2.3. Milestone 3

- #### `End goal:`
    - Uploading the data to a mySQL database (hosted locally or on a cloud service provider of choice)

- #### `Deliverables:`
    1. A mySQL database containing the processed data from the JSON file. (if possible, the database should be hosted on a cloud service provider)
    2. Adding a Python function that uploads the data to the mySQL database (ensuring that the data is uploaded to the correct tables, and that the primary and foreign keys attributes are set correctly, and all columns have the correct data types)

- #### `Actions to do:`
    1. research cloud service providers to find the best choice (to avoid incurring costs, I'll probably use the free tier of the service provider)
    2. All items in the `Deliverables` section of Milestone 3.

- #### `Timeframe:`
    - 2<sup>nd</sup> day.

- #### `Status:`
    - Not started.
  
### 2.4. Milestone 4
- to be added later.