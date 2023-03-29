

# Import the necessary modules
from kafka import KafkaConsumer
from pymongo import MongoClient
import json

# Connect to MongoDB and icecream_data database
try:
   client = MongoClient('localhost',27017)               # Make sure that your mongodb server is running
   db = client.faker_bikeprice_data
   print("Connected successfully!")
except:  
   print("Could not connect to MongoDB")
    
# connect kafka consumer to desired kafka topic	
consumer = KafkaConsumer('bike-price-faker',bootstrap_servers=['localhost:9092'])

# ----------------------------------------------------------------------------------------------------------------------------------------

# Parse received data from Kafka
for msg in consumer:
   #print('message:', msg)
   record = json.loads(msg.value)
   id = record['id']
   name = record['name']
   phoneNumber = record['phoneNumber']
   address = record['phoneNumber']
   location = record['Location']
   brand = record['brand']
    
    # Create dictionary and ingest data into MongoDB
   try:
      bikeprice_rec = {'id': id, 'name':name, 'phoneNumber':phoneNumber, 'address':address, 'location' :location, 'brand': brand}
      print('icecream_rec:', bikeprice_rec)
      rec_id = db.faker_bikeprice_info.insert_one(bikeprice_rec)
      print("Data inserted with record ids", rec_id)
   except:
      print("Could not insert into MongoDB")