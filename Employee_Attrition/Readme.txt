# Data Pipeline Project

- This Project is created to test the learners on their understanding with respect to Data Pipeline
- In this project the Input data is stored in MySQL DB and PostgreSQL database.
- In real time the source data to be analysed might be stored in different databases for better accessibility.
- In this project MySQL and PostgreSQL database is chosen as a storage place.
- For this project Extract Transform and Load(ETL) process is followed. 
- The data is going to be extracted from MySQL database using a Messaging Queue - Apache Kafka.
- There are lots of Message Queues available, for this project we are using Apache Kafka(Most Popular and better throughput)
- The data ingested from MySQL DB will be pushed from kafka-consumer to the Jupyter notebook for further Transformation.
- The second data is stored locally and will be ingested using Python library - Pandas
- Both the ingested data (data1 and data2) are merged and pre-processed using Python library.
- Visualisation reports are generated for pre-processed data.
- Storing the final pre-processed data at data-warehouse (MongoDB)