

# Import the below libraries
import random                               # For random genration of data
from faker.providers import BaseProvider    # Helps to provide fake data 
from faker import Faker
import json
from kafka import KafkaProducer
import time
import random
import argparse

#Note: Fake data is useful for someone who is learning to process data and pipelining data from one system to another.

# - Adding a Bike provider with 2 methods:
#   * bike_brands for selecting the bike based on that.
#   * bike_location to know from which location it was bought.

# ----------------------------------------------------------------------------------------------------------------------------------------

# The class that is used here is the source data which will be randomly selected when called

class BikeProvider(BaseProvider):                               # Defining a base class
# Some top brands of bike
    def bike_brand(self):
        top_bike_brands = [                                     
                            'TVS',
                            'Bajaj',
                            'Yo',
                            'Honda',
                            'Mahindra',
                            'Hero',
                            'Yamaha',
                            'Suzuki',
                            'Activa',
                            'Vespa'
                            'Royal',
                            'Benelli',
                            'KTM',
                            'UM',
                            'Kawasaki',
                            'Hyosung',
                            'BMW',
                            'Harley'
                            ]
        return top_bike_brands[random.randint(0, len(top_bike_brands)-1)]
    
# Some bike locations
    def bike_location(self):
        bike_locations =   [
                            'Chennai',
                            'Bangalore',
                            'Hydrabad',
                            'Cochin',
                            'Vizag',
                            'Mumbai',
                            'Ahamedabad',
                            'Kolkatta',
                            'Chandigarh',
                            'Delhi',
                            'Pune',
                            'Bhubhaneshwar',
                            'Agra',
                            'Meghalaya',
                            'Pondichery',
                            'Goa'
                            ]
        return bike_locations[random.randint(0, len(bike_locations)-1)]
    
# ---------------------------------------------------------------------------------------------------------------------------------------
# Basic parameter related to the order
MAX_NUMBER_BIKES_IN_ORDER = 5

# Creating a Faker instance and seeding to have the same results every time we execute the script
fake = Faker()
Faker.seed(42)

# Adding the newly created PizzaProvider to the Faker instance
fake.add_provider(BikeProvider)

# ---------------------------------------------------------------------------------------------------------------------------------------
# Creating function to generate the Bike Order

def produce_bike_order(ordercount = 1):
    brand = fake.bike_brand()                               # Setting icecream_shop with the defined shop class
    
    # message composition
    message = {
        'id': ordercount, # Auto increment by i=i+1
        'name': fake.unique.name(),
        'phoneNumber': fake.unique.phone_number(),
        'address': fake.address(),
        'Location': fake.bike_location(),
        'brand': fake.bike_brand()
    }
    key = {'brand': brand}
    return message, key                                     # Returning as key value pair - better for storing in NoSQL Database

# --------------------------------------------------------------------------------------------------------------------------------------
# produce_msgs function starts producing messages with Faker
def produce_msgs(hostname='localhost',
                 port='9092',
                 topic_name='bike-price-faker',      # Name of the topic created in kafka
                 nr_messages=626,                    # Number of messages to produce (0 represents unlimited)
                 max_waiting_time_in_sec=10):
    
    # Function for Kafka Producer with certain settings related to the Kafka's Server
    producer = KafkaProducer(
                bootstrap_servers=hostname+":"+port,
                value_serializer=lambda v: json.dumps(v).encode('ascii'),               # Encoding the producer data
                key_serializer=lambda v: json.dumps(v).encode('ascii')
                )
    
    # When the number of messages are 0 or less then it is defined as infinite
    if nr_messages <= 0:
        nr_messages = float('inf')
    
    i = 0                                        # Setting the initial number of orders to be zero
    while i < nr_messages:
        message, key = produce_bike_order(i)     # Getting the key and message from the function which is used for generating the bike order

        print("Sending: {}".format(message))
        # sending the message to Kafka
        producer.send(topic_name,
                      key=key,
                      value=message)
        
        # Sleeping time / Waiting time
        sleep_time = random.randint(0, max_waiting_time_in_sec * 10)/10
        print("Sleeping for..."+str(sleep_time)+'s')
        time.sleep(sleep_time)

        # Force flushing of all messages
        if (i % 100) == 0:
            producer.flush()
        i = i + 1
    producer.flush()

# calling the main produce_msgs function: parameters are:
#   * nr_messages: number of messages to produce
#   * max_waiting_time_in_sec: maximum waiting time in sec between messages

produce_msgs()