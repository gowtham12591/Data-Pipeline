# Import the necessary modules
from kafka import KafkaConsumer
import pandas as pd
import json
from io import StringIO
    
# connect kafka consumer to desired kafka topic	
consumer = KafkaConsumer('employee_attrition',bootstrap_servers=['localhost:9092'])

# Reading the data from the topic employee_attrition
for details in consumer:
    values = details.value      # Reading only the values from the consumer data
    print(type(values))

    # Obtained data is in the form of bytes
    # Converting bytes data to string
    s=str(values,'utf-8')        # Decoding the serialized data
    data = StringIO(s) 

    # Converting the string data to list
    for i in data:
        new_list1 = json.loads(i)

    # Converting the list to the dataframe
    df = pd.DataFrame(new_list1, columns=['EmployeeNumber', 'Age', 'Attrition', 'Business_Travel', 'DailyRate', 'Department', 
                                    'Distance_From_Home', 'Education_in_years', 'Education_Field', 'Emp_count'])
    print(df.head())

    # Saving the dataframe for future use
    df.to_csv('data/df1.csv', index=False)