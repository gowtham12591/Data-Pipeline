# Import the necessary modules
from kafka import KafkaConsumer
import pandas as pd
import json
from io import StringIO
    
# connect kafka consumer to desired kafka topic	
consumer = KafkaConsumer('employee_attrition_psql',bootstrap_servers=['localhost:9092'])

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
    df = pd.DataFrame(new_list1, columns=['EmployeeNumber', 'Environment_Satisfaction', 'Gender', 'Hourly_Rate',
                                        'Job_Involvement', 'Job_Level', 'Job_Role', 'Job_Satisfaction', 'Marital_Status',
                                        'Monthly_Income', 'Monthly_Rate', 'Num_comp_worked', 'Age_Over18', 'Work_Overtime',
                                        'Salary_Hike_Percentage', 'Performance_Rating', 'Relationship_Satisfaction',
                                        'Std_working_hours', 'Stack_Option_Level', 'Total_working_years', 'Train_time_last_year',
                                        'work_life_balance', 'Years_company', 'Years_current_role', 'Years_Promotion',
                                        'Years_curr_mangaer'])
    print(df.head())

    # Saving the dataframe for future use
    df.to_csv('data/df2.csv', index=False)