
# Kafka Consumer

# Import the necessary modules
from kafka import KafkaConsumer
from pymongo import MongoClient
import json

# Connect to MongoDB and bike_price_data database
try:
   client = MongoClient('localhost',27017)               # Make sure that your mongodb server is running
   db = client.bike_price_data
   print("Connected successfully!")
except:  
   print("Could not connect to MongoDB")
    
# connect kafka consumer to desired kafka topic	
consumer = KafkaConsumer('bike-price',bootstrap_servers=['localhost:9092'])


# Parse received data from Kafka
for msg in consumer:
   #print('message:', msg)
   record = json.loads(msg.value)
  
   bike_id = record['bike_id']
   Brand = record['Brand']
   Model = record['Model']
   Selling_Price = record['Selling_Price']
   year = record['year']
   Seller_Type = record['Seller_Type']
   Owner = record['Owner']
   KM_Driven = record['KM_Driven']
   Ex_Showroom_Price = record['Ex_Showroom_Price']
    
    # Create dictionary and ingest data into MongoDB
   try:
      bike_price_rec = {'bike_id':bike_id, 'Brand':Brand, 'Model':Model, 'Selling_Price':Selling_Price, 'year':year, 
                        'Seller_Type':Seller_Type, 'Owner':Owner, 'KM_Driven':KM_Driven, 'Ex_Showroom_Price':Ex_Showroom_Price}
      #print('bike_price_record:', bike_price_rec)
      rec_id = db.bike_price_info.insert_one(bike_price_rec)
      print("Data inserted with record ids", rec_id)
   except:
      print("Could not insert into MongoDB")