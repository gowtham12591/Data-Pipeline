### Pushing the data from MYSQL Database to Mongo DB using Apache Kafka  - (Windows Machine)

Step 1: Data Creation  

    - Create a database in MYSQL DB called bike_price_db
    - Create a table with the name bike_price and add the details as referenced in bike_price.sql
    - Now import the 'BikePrices.csv' file inside the created table
    - Check whether the data is imported corrctly using the command as referenced in bike_price.sql   

Step 2: Initializing Apache-Kafka & Creating a topic        

    - To connect MYSQL DB to jupyter notebook use,
        pip install mysql-connector-python
    - To stream the data kafka is utilized, Install kafka using 
        pip install kafka-python
    - Apart from this Apache-Kafka has to be installed seperately in the system. Please refer to the below link for easy installation,
        - https://kafka.apache.org/downloads
        - https://medium.com/@sangeethaprabhagaran/creating-a-kafka-topic-in-windows-e51b15e5ccd4 
    - Once kafka is installed, zookeeper and kafka-server has to be started
    - Navigate to the place where kafka folder is saved in command prompt,
    - Start zookeeper and kafka-server using
        - .\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
        - .\bin\windows\kafka-server-start.bat .\config\server.properties
    - Create a topic using,
        - bin\windows\kafka-topics.bat --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic bike-price
        - Here the topic name chosen is 'bike-price' with 1 partition and 1 replication factor**, replication factor and partition can be 
          any value based on the need.

Step 3: Stream the data from MYSQL DB to Kafka topic
    - Refer the code in mysql_producer.py
    - Open command prompt and navigate to the folder where mysql_producer.py is saved and run the below command,
        python mysql_producer.py

Step 4: MongoDB installation
    - Install pymongo using '!pip install pymongo'
    - Apart from this MongoDB has to be installed seperately, please refer to the below official page for downloading the software
    - https://www.mongodb.com/docs/manual/tutorial/install-mongodb-on-windows-unattended/
    - If you are finding it difficult please refer to some youtube videos for installation and how to turn on the mongodb server
    - Open MongoDB compass and connect it to the server 

Step 5: Push the data from Kafka topic to MongoDB
    - Refer the code in mysql_consumer.py
    - Open command prompt and navigate to the folder where mysql_consumer.py is saved and run the below command,
        python mysql_consumer.py

Step 6: Check for database in MongoDB
    - Open MongoDB compass and check for the database named bike_price_data and check for the collection bike_price_info
    - Open the collection in a seperate tab and verify the data with the MYSQL database

