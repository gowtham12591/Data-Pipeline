# Data Preprocessing:
# Reading the dataset from MongoDB obtained from two different sources and merging it together
# The merged dataset can be used for further analysis using Tableau or Model building

## PyMongo is a Python library containing tools for working with MongoDB.
from pymongo import MongoClient
client = MongoClient("mongodb://localhost:27017/")  # Connecting with Mongodb server and creating a client to interact with mongodb server.

## To view the list of databases names available in the mongodb.
client.list_database_names()

### Here we are going to utilize bike_price_data and faker_bikeprice_data
db = client.bike_price_data
db1 = client.faker_bikeprice_data

# Checking the collection names
print(db.list_collection_names())
print(db1.list_collection_names())

# printing a sample data from the first collection 
data1 = db['bike_price_info']
print('bike_price_info:', data1.find_one())
print('-'*150)
data2 = db1['faker_bikeprice_info']
print('faker_bikeprice_info:', data2.find_one())

# print the total number of documents in the collection
print ("total docs in bike_price collection:", data1.count_documents( {} ))
print ("total docs in faker_bikeprice collection:", data2.count_documents( {} ))

# print the total number of documents returned in a MongoDB collection
cursor1 = data1.find()
data1_list = list(cursor1)
print(data1_list[0:5])
print ("Total docs returned by find() for bike_price_data:", len(data1_list))
print('-'*150)

cursor2 = data2.find()
data2_list = list(cursor2)
print(data2_list[0:5])
print ("Total docs returned by find() for faker_bikeprice_data:", len(data2_list))

# Converting the list to dataframe

import numpy as np
import pandas as pd
df1 = pd.DataFrame(data1_list) 
df2 = pd.DataFrame(data2_list)

# Dropping the un-necessary columns
df1 = df1.drop(columns = ['_id', 'bike_id'], axis = 1)
df2 = df2.drop(columns = ['_id', 'id'], axis=1)

# Renaming the column name in dataframe-2 so that dataframe-1 and dataframe-2 can be merged
df2 = df2.rename(columns={'brand': 'Brand'})

# Merging the two dataframes based on the 'Brand'
df = pd.merge(df1, df2, on='Brand')
print(df.head())

# Saving the merged data from further analysis in Vizualization software
df.to_csv('Bike_Price.csv')