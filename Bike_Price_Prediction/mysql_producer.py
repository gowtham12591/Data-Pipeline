# Pusing the data from MYSQL Database to Kafka Topic

# Import the required libraries
import numpy as np
import pandas as pd
from kafka import KafkaProducer, KafkaConsumer      # importing producer and consumer for data transformation
import mysql.connector                              # To connect with mysql server
from json import dumps                              # To serialize the data
import time

# Mysql database connection with jupyter notebook
connection = mysql.connector.connect(
                                    host = 'localhost', 
                                    database = 'bike_price_db',
                                    user = 'root',
                                    password = 'password@123'
                                    )

# Set the cursor to fetch all the records from the bike_price table
cursor = connection.cursor()
cursor.execute('select * from  bike_price')
record = cursor.fetchall()
cursor.close()
connection.close()

# ---------------------------------------------------------------------------------------------------------------------

# Kafka Producer

# Create an instance of the KafkaProducer connecting to local kafka server and JSON value serializer
producer = KafkaProducer(
                        bootstrap_servers=["localhost:9092"],
                        value_serializer= lambda m: dumps(m).encode('utf-8')
                        )

# Loop through the records and set the data to be a json data and send the json data to kafka topic bike-price
# Data is stored in the form of key:value pair
for i in record:
	data={'bike_id':i[0], 'Brand':i[1], 'Model':i[2], 'Selling_Price':i[3], 'year':i[4], 'Seller_Type':i[4],  
       	  'Owner':i[5], 'KM_Driven':i[6], 'Ex_Showroom_Price':i[7]}
	producer.send('bike-price', data)            # Here the topic name is 'bike-price'
	time.sleep(1)
	

# -------------------------------------------------------------------------------------------------------------------------


