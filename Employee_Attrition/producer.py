# Import the required libraries

import numpy as np
import pandas as pd
from kafka import KafkaProducer, KafkaConsumer      # importing producer and consumer for data transformation
import mysql.connector                              # To connect with mysql server
from json import dumps                              # To serialize the data
import time

# Mysql database connection
connection = mysql.connector.connect(
                                    host = 'localhost', 
                                    database = 'emp_attrition_db',
                                    user = 'root',
                                    password = 'password@123'
                                    )

# Set the cursor to fetch all the records from the employee table
cursor = connection.cursor()
cursor.execute('select * from  emp_details')
record = cursor.fetchall()
cursor.close()
connection.close()

# Obtained data from the cursor will be in the form of list
# Converting the list to dataframe
df = pd.DataFrame(record, columns=['EmployeeNumber', 'Age', 'Attrition', 'Business_Travel', 'DailyRate', 'Department', 
                                   'Distance_From_Home', 'Education_in_years', 'Education_Field', 'Emp_count'])
print(df.sample(5))

#create an instance of the KafkaProducer connecting to local kafka server and JSON value serializer
producer = KafkaProducer(
                        bootstrap_servers=["localhost:9092"],
                        value_serializer= lambda m: dumps(m).encode('utf-8')    # Encoding the data to 'utf-8' format
                        )


# Transferring the data to Kafka Topic
producer.send("employee_attrition", record)
producer.flush()
time.sleep(3)
