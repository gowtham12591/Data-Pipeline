# Pusing the final cleansed dataset to Datawarehouse(MongoDB)

# Import required libraries
import pandas as pd
from pymongo import MongoClient
import json

# Reading the final cleansed dataset
df = pd.read_csv('cleansed_data.csv')

try:
    client = MongoClient('localhost',27017)               # Make sure that your mongodb server is running
    db1 = client.employee_attrition                       # 'employee_attrition' is the name of the database
    print("Connected successfully!")
except:  
    print("Could not connect to MongoDB")


# Collection_name
retail_rec = df.to_dict(orient='records')                 # Converting the dataframe tpo dictionary
try:
    rec_id = db1.emp_info.insert_many(retail_rec)         # emp_info is the name of the collection
    print("Data inserted with record ids", rec_id)
except:
    print("Could not insert into MongoDB")
