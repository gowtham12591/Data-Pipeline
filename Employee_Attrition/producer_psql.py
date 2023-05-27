# Import the required libraries

import numpy as np
import pandas as pd
from kafka import KafkaProducer                     # importing producer and consumer for data transformation
# pip3 install psycopg2
# For mac m1 users if you get error ref below link for solution
# ref:https://stackoverflow.com/questions/73625820/error-importing-psycopg2-on-m1-mac-incompatible-architecture/73700246
import psycopg2                                     # To connect with postgresql server
from json import dumps                              # To serialize the data
import time

# Mysql database connection
connection = psycopg2.connect(
                                host="localhost",
                                database="employee_attrition",
                                user="postgres",
                                password="password123")

# Set the cursor to fetch all the records from the employee table
cursor = connection.cursor()
cursor.execute('select * from  emp_details')
record = cursor.fetchall()
cursor.close()
connection.close()

# Obtained data from the cursor will be in the form of list
# Converting the list to dataframe
df = pd.DataFrame(record, columns=['EmployeeNumber', 'Environment_Satisfaction', 'Gender', 'Hourly_Rate',
                                    'Job_Involvement', 'Job_Level', 'Job_Role', 'Job_Satisfaction', 'Marital_Status',
                                    'Monthly_Income', 'Monthly_Rate', 'Num_comp_worked', 'Age_Over18', 'Work_Overtime',
                                    'Salary_Hike_Percentage', 'Performance_Rating', 'Relationship_Satisfaction',
                                    'Std_working_hours', 'Stack_Option_Level', 'Total_working_years', 'Train_time_last_year',
                                    'work_life_balance', 'Years_company', 'Years_current_role', 'Years_Promotion',
                                    'Years_curr_mangaer'])
print(df.sample(5))

#create an instance of the KafkaProducer connecting to local kafka server and JSON value serializer
producer = KafkaProducer(
                        bootstrap_servers=["localhost:9092"],
                        value_serializer= lambda m: dumps(m).encode('utf-8')    # Encoding the data to 'utf-8' format
                        )


# Transferring the data to Kafka Topic
producer.send("employee_attrition_psql", record)
producer.flush()
time.sleep(3)