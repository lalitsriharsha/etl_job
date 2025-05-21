Kafka ETL Pipeline with Cassandra
=======================================

This project implements an ETL pipeline in Python using Kafka for messaging and Cassandra for storage. It processes patient data from a CSV file by cleaning, transforming, and loading it into a Cassandra table.

Project Structure
-----------------
project/
│
├── main.py                            
├── app/
│   ├── config/
│   │   └── config.py                  
│   ├── controller/
│   │   ├── producer_transformed.py    
│   │   └── consumer_loader.py         
│   ├── service/
│   │   ├── cleaner.py                 
│   │   └── transformer.py             
│   ├── utils/
│   │   ├── deduplicator.py            
│   │   └── kafka_helper.py            
│
├── resources/
│   ├── raw_data/patients.csv          
│   └── transformed/final_transformed.csv  

Technologies Used
-----------------
- Python 3.8+
- Kafka (via aiokafka)
- Apache Cassandra
- Pandas
- Asyncio

How It Works
------------
1. Producer (`producer_transformed.py`)
    - Reads `patients.csv`.
    - Cleans and transforms data.
    - Saves transformed data to CSV.
    - Produces to Kafka topic `etl_transformed`.

2. Consumer (`consumer_loader.py`)
    - Consumes from Kafka.
    - Inserts data into Cassandra table `etl.etl_transformed`.

Setup Instructions
------------------

1. Start Kafka and Cassandra Locally

   Kafka:
     - Start Zookeeper
   ```bash
       .\bin\windows\zookeeper-server-start.bat  .\config\zookeeper.properties
   ```
   
     - Start Kafka
    ```bash
    .\bin\windows\kafka-server-start.bat .\config\server.properties
    ```
   Cassandra:
     - cassandra
    ```bash
    start-cassandra.bat
    ```

2. Install Python Dependencies
```bash
   pip install -r requirements.txt
```
   If `requirements.txt` is not available:
```bash
   pip install aiokafka pandas cassandra-driver python-dateutil
```
Run the ETL Pipeline
--------------------
python main.py

Output
------
- CSV saved at: `resources/transformed/final_transformed.csv`
- Data inserted into Cassandra under keyspace `etl` and table `etl_transformed`

Cleaning & Transformation Details
---------------------------------
- Email: Validated and masked (e.g., j***@domain.com)
- Phone: Validated and masked (e.g., ***-***-1234)
- Date: Standardized to YYYY-MM-DD
- Drop Columns: GENDER, YOB, home, RACE, initial, region
- Drop Rows: >50% missing values

