# End-to-end Real Time Streaming Pipeline 
This project is a real-time streaming pipeline that processes transaction data using Apache Kafka, Apache Spark, Hive, and MySQL technologies. The Berka dataset is utilized for simulating real-life transactions. We have stored the data in an AWS S3 bucket and created python code to send the data in random intervals to the Kafka Producer in order to simulate real-life transactions. The pipeline captures transaction data at random intervals and performs various transformations, aggregations, and reporting in real-time.

## About the Dataset
The dataset used for this project is the Berka dataset, which is a real-world dataset containing financial transaction data. It was originally created for the purposes of a data mining competition organized by the Czech Technical University in Prague. The dataset represents a comprehensive record of financial transactions involving bank accounts and related information.


## Architecture

The main components of the pipeline are as follows:

1. Kafka: Randomly generates transaction data and sends it to the Kafka cluster to simulate real-time events. Acts as the message queue where transaction data is ingested and distributed to Spark Streaming for further processing.
2. Spark Streaming: Receives the transaction data from Kafka and performs real-time transformations and aggregations. Additionally, it merges data from MySQL database tables for more comprehensive reporting.
3. MySQL Database: Contains various tables, such as account, client, and district, which are used to enrich the transaction data during the streaming process.
4. Hive: Stores the aggregated data in real-time, providing an efficient and flexible data warehousing solution for querying and reporting.

<br>
<p align="center">
	<img src="img/architecture.png" width='90%'><br><br>
    <em>Data Pipeline Architecture</em>
</p>



### Advantages of the Architecture

1. Real-Time Processing: The architecture enables real-time processing of transaction data, allowing for up-to-date and dynamic reporting.
2. Scalability: Kafka, Spark, and Hive are scalable technologies, capable of handling large-scale data processing and storage requirements.
3. Flexibility: The use of Hive as a data warehouse allows for flexible querying and reporting on the aggregated data.
4. Enrichment: The project enriches the transaction data with additional information from MySQL tables, enhancing the quality and depth of insights.
5. Efficient Data Storage: Hive and Hadoop HDFS efficiently store and manage large-scale data, providing a reliable and robust data storage solution.

This architecture provides a comprehensive and efficient solution for processing, analyzing, and reporting real-time transaction data, making it ideal for various data analysis scenarios.

## Prerequisites

Before running the project, ensure you have the following components installed:

1. Apache Kafka
2. Apache Spark
3. Hive
4. MySQL
5. Python
6. PySpark
7. AWS S3 Bucket loaded with the .csv files

### Databases setup:

#### MySQL
1. Create a database named berka. 
2. In berka database, create tables account,client, district , loan, dispo, card: 
3. Once the tables are created, build the parent-child relationships between them using ALTER TABLE <Table1>
ADD FOREIGN KEY (<Table1_ID>) REFERENCES <Table2>(<Table2_ID>);
4. Exit mysql and run load_data.py

#### Hive
1. Use the default database.
2. Create tables monthly_trend(transaction_month int, transaction_count int, transaction_amount int),  yearly_trend(transaction_month int, transaction_count int, transaction_amount int), trans_by_age_gender(age_levels string, gender string, avg_transactions int)
3. Use ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\|'
4. You can use SHOW TABLES; to double check that the table was created

## Running the Project:

### 1. Terminal 1 - Start Kafka Zookeeper


~$ cd kafka/
~/kafka$ bin/zookeeper-server-start.sh config/zookeeper.properties

    
#### 2. Terminal 2 - Start Kafka Broker


~$ cd kafka/
~/kafka$ bin/kafka-server-start.sh config/server.properties


#### 3. Terminal 3 - Create Kafka topic
First check if any topics exist on the broker (this should return nothing if you just started the server)
    

~/kafka$ bin/kafka-topics.sh --list --bootstrap-server localhost:9092


Now make a new topic named trans


~/kafka$ bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic trans


Run the first command again to make sure the topic was successfully created.
    
Clear the terminal using clear
    
Start Hadoop cluster using the following commands

~$ hdfs namenode -format
~$ start-dfs.sh

#### 4. Terminal 4 - Hive Metastore
    

~$ hive --service metastore


#### 5. Terminal 5 - Hive
In order to make sure the transformed data is being stored in Hive, we need to enter the Hive shell so that we can query the tables. Write a query to count the records in the tweets table. This should return nothing, but as we start the stream producer/consumer, we can use the up-arrow to run this query again to check if the data is coming through.
    

~$ hive
...
hive> use default;
hive> select count(*) from tweets;


#### 6. Terminal 6 - Stream Producer
In this new terminal, we are going to run the stream producer. Mine is named Kafka_Producer.py, but just use whatever you named yours.

If want to change the file permissions with chmod 755 Kafka_Producer.py, you can run the stream producer with a simple ./


~$ ./Kafka_Producer.py


This script should produce output to the console everytime a transaction is sent to the Kafka cluster, so you should be able to know whether or not the stream producer is working. 

Keep this running and open up a new terminal.
   
#### 7. Terminal 7 - Stream Consumer + Spark Transformer
Now we are ready to run the consumer. Since we want to run it as a Spark job, we'll use spark-submit
    

~$ spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,mysql:mysql-connector-java:5.1.49 Spark_Transformer.py


This should produce a lot of logging output. Keep this running as long as you want the example to be running.

### Did It Work?
If you were able to run the producer script and the spark transformer, things should be working correctly! You should be able to see small dataframes being printed to the console in the terminal where the spark transformer is running.

Next, go back to "Terminal #5 (Hive shell), and run the select count(*) to see if the data is being written to Hive. If you get something greater than zero, it's working!

## How My Terminals Looked

<br>
<p align="center">
	<img src="img/schemas.png" width='90%'><br><br>
    <em>Schemas of tables printed in Spark-Shell</em>
</p>

<br>
<p align="center">
	<img src="img/stream.png" width='90%'><br><br>
    <em>Live stream of data being sent to Kafka Consumer</em>
</p>

<br>
<p align="center">
	<img src="img/t1.png" width='90%'><br><br>
    <em>Spark Transformating and Sending To Hive - 1</em>
</p>

<br>
<p align="center">
	<img src="img/t2.png" width='90%'><br><br>
    <em>Spark Transformating and Sending To Hive - 2</em>
</p>

<br>
<p align="center">
	<img src="img/live.png" width='90%'><br><br>
    <em>Demo</em>
</p>

## Conclusion
Congratulations! You now have a real-time streaming pipeline that processes transaction data using Kafka, Spark, Hive, and MySQL. This pipeline enables you to perform real-time aggregations and reporting, making it ideal for various data analysis scenarios. Happy streaming!
