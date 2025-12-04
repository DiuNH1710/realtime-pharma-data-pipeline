# 💊 Real-Time Healthcare Data Streaming Pipeline
This project implements a **real-time ingestion, transformation, and analytics pipeline** for pharmaceutical data in Vietnam.
It automatically **crawls, streams, processes, and stores official drug information** from the Vietnamese pharmaceutical regulatory portal:

🔗 https://dichvucong.dav.gov.vn/congbothuoc/index
![img_3.png](images%2Fimg_3.png)

The system is designed for incremental crawling, low-latency processing, and scalable transformations, making it suitable for dashboards, monitoring systems, and analytical workloads.

## 🚀 Tech Stack

- **Apache Airflow** – schedules and orchestrates crawling + ETL workflows

- **Apache Kafka** – handles real-time streaming of newly crawled records

- **Apache Spark (Structured Streaming)** – performs large-scale transformations, enrichment, and validation

- **PostgreSQL** – structured storage with staging and production tables

- **Docker Compose** – orchestration for all services (Airflow, Kafka, Spark, Postgres, etc.)

- **Python** – crawling scripts, data cleaning, and integration logic

## 📦 Data Description

The pipeline extracts detailed information about drugs approved for circulation in Vietnam.
Each record includes:

- Registration number (soDangKy)

- Drug name (tenThuoc)

- Active ingredients

- Manufacturer (tenCongTySanXuat)

- Country of manufacture

- Registration date (ngayCapSoDangKy)

- Dosage form / formulation

- Packaging info

- ATC code, route of administration, and other regulatory metadata
![img_7.png](images%2Fimg_7.png)

## 🎯 What This Project Enables

- Daily or scheduled incremental crawling

- Real-time streaming via Kafka

- On-the-fly data transformation and quality checks

- Staging → production upsert strategy in PostgreSQL

- Containerized systems for reproducibility and scaling

- Clear separation of ingestion, processing, and storage layers
## 📁 Project Structure
```
DataStreamingFromDVCVer2/
│
├── dags/ # Airflow DAG definitions
│ ├── dvc_flow.py # Main DAG for handling DVC-triggered data
│ └── upsert_dag.py # DAG for upserting data to main DB
│
├── manager_crawl_time/ # Manage crawl timestamps
│ └── manager_time.py # Handles reading & writing crawl time from DB
│
├── pipelines/ # DVC data pipeline scripts
│ └── dvc_pipeline.py
│
├── postgres_data/ # (Optional) Postgres init SQL or volumes
│
├── realtime_processor/
│ ├── db/ # DB connection, creation, upsert logic
│ ├── kafka/ # Kafka consumer logic
│ ├── schema/ # PySpark schema definition
│ └── spark/ # Spark stream connection and transform logic
│ ├── connection.py # SparkSession connection
│ └── transform.py # Transform logic (JSON parse, explode, etc.)
│
├── script/ # Utility or helper scripts
│
├── docker-compose.yml # Full Docker environment (Kafka, Airflow, Postgres, etc.)
├── init.sql # SQL for DB setup
├── requirements.txt # Python dependencies
└── .gitignore

```

## 🚀 How It Works
The pipeline consists of multiple stages, from crawling the official portal to streaming, processing, and storing the data in PostgreSQL. Below is a detailed explanation of each stage with suggested visuals and code snippets

1. **Airflow DAG (`dvc_flow.py`)** is triggered periodically:
   - Trigger: The DAG is scheduled by Airflow (e.g., daily at 8:00 AM).
      ![img_6.png](images%2Fimg_6.png)
   - Step 1: Python script calls the Vietnamese drug portal API or scrapes HTML to fetch the latest drug records.
   - Step 2: Compare each record's lastModificationTime with the last crawl timestamp stored in the DB. Only new or updated records are processed further.
   - Step3: Sends only new/updated records to Kafka topic `all_data`
      ```python
      
      def extract_dvc_data():
          job_name = "extract_data_from_DVC"
          last_crawl_time = get_last_crawl_time(job_name)
      
          if last_crawl_time:
              if last_crawl_time.tzinfo is None:
                  last_crawl_time = last_crawl_time.replace(tzinfo=timezone.utc)
      
          total_count = get_total_count()
          if total_count is None:
              raise ValueError("Total count is not available.")
          # all_data = []
          skip_count = 0
      
          producer = KafkaProducer(bootstrap_servers=['kafka:9092'], max_block_ms=5000)
          while skip_count < 1000:
              # total_count+100:
      
              try:
                  data = get_data_per_page(skip_count)
                  if data is None:
                      print(f"Skipping page with skip_count {skip_count} due to failure")
                      time.sleep(5)  # Wait before retrying
                      continue
                  data_formated = format_data(data, last_crawl_time)
                  print(f"Fetched {skip_count + 100}/{total_count}")
      
                  skip_count += 100
                  time.sleep(1)  # Adjust as needed
      
                  json_rows = json.dumps(data_formated, ensure_ascii=False).encode('utf-8')
      
                  producer.send('all_data', json_rows)
                  producer.flush()
                  print('message sent')
              except Exception as e:
                  logging.error(f'An error occured: {e}')
                  continue
      ```
         

2. **Kafka Producer** (in DAG) streams filtered data in JSON format.
- Kafka acts as the message broker to decouple the crawling process from downstream processing.
- Any consumer (like Spark) can subscribe to the topic all_data and process records in near real-time.

   ![img_8.png](images%2Fimg_8.png)

3. **Spark Structured Streaming** (inside `spark/main.py`) reads from Kafka:
   - Spark Structured Streaming reads from Kafka continuously.

   - JSON records are parsed according to the predefined schema and transformed into a normalized DataFrame.
    ![img_9.png](images%2Fimg_9.png)
    ![img_10.png](images%2Fimg_10.png)
   
4. **Spark → PostgreSQL**
   - Spark writes transformed batch data to PostgreSQL via JDBC
   ```python
   
   def write_to_postgres(batch_df, batch_id, postgres_table_name):
       print(f"Processing batch {batch_id}")
       batch_df = batch_df.drop("id")
       batch_df = batch_df.dropDuplicates(["soDangKy"])
   
       batch_df.show(1)  # Show the first 1 rows of the batch for inspection
       # use localhost or postgres??
   
       batch_df.write \
           .format("jdbc") \
           .option("url", "jdbc:postgresql://host.docker.internal:5432/postgres")\
           .option("dbtable", f"public.{postgres_table_name}") \
           .option("user", "postgres") \
           .option("password", "123456") \
           .mode("append") \
           .save()
      ```
   - Data is inserted or upserted using custom logic in `upsert_to_main_table.py`
   
   ```python
      
   def upsert_to_main_table():
       conn = None
       try:
           # connect to PostgreSQL
           conn = psycopg2.connect(
               dbname="postgres",
               user="postgres",
               password="123456",
               host="host.docker.internal",
               port="5432"
           )
           cursor = conn.cursor()
   
           cursor.execute("""
               INSERT INTO pharmaceutical_data AS main
               
               SELECT DISTINCT ON (soDangKy) * 
               FROM pharmaceutical_data_staging
               ORDER BY soDangKy, lastModificationTime DESC
               ON CONFLICT (soDangKy)
               
               DO UPDATE SET
                   idThuoc = EXCLUDED.idThuoc,
                   phanLoaiThuocEnum = EXCLUDED.phanLoaiThuocEnum,
                   tenCongTyDangKy = EXCLUDED.tenCongTyDangKy,
                   diaChiDangKy = EXCLUDED.diaChiDangKy,
                   nuocDangKy = EXCLUDED.nuocDangKy,
                   congTyDangKyId = EXCLUDED.congTyDangKyId,
                  ...
   
               """)
   
           print("Table upsert successfully in PostgreSQL")
           cursor.execute("TRUNCATE TABLE pharmaceutical_data_staging;")
           print("Truncated staging table successfully")
   
           conn.commit()
           cursor.close()
       except (Exception, psycopg2.DatabaseError) as error:
           print(f"Error: {error}")
       finally:
           if conn is not None:
               conn.close()
   ```
5. **Airflow DAG (`upsert_dag.py`)** updates the main table in DB with new records.
![img_14.png](images%2Fimg_14.png)
---

## 🧪 How to Run

> Make sure you have **Docker Desktop** installed.

### 1. Start All Services

```
docker-compose up --build
```
![img_15.png](images%2Fimg_15.png)

### 2. Access Services
Airflow UI: http://localhost:8080
![img_16.png](images%2Fimg_16.png)

### 3. Start Kafka & create topic all_data

Open a terminal inside the Kafka container:
```
docker exec -it kafka kafka-topics --create --topic all_data --bootstrap-server kafka:9092 --partitions 1 --replication-factor 1
```

Verify topic exists:
```
docker exec -it kafka kafka-topics --list --bootstrap-server kafka:9092
```
![img_5.png](images%2Fimg_5.png)


### 4. Setup Spark and PostgreSQL Integration
🐘 Install PostgreSQL driver inside Spark container
```
docker exec -it spark-master pip install psycopg2-binary
```
#### 🗄️ Initialize Database Schema
```
docker exec -it spark-master python /opt/bitnami/spark/realtime_processor/db/init_db.py
```
#### ⚡ Start Spark Streaming Job
```
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077   /opt/spark/realtime_processor/main.py

```
### 5. Trigger DAGs via Airflow
Open Airflow UI at http://localhost:8080

Trigger:

#### 1. Open Airflow UI at http://localhost:8080

#### 2. Trigger dvc_flow DAG – this will:

- Crawl new data from the official portal

- Filter incremental updates

- Stream data into Kafka topic all_data

- Transform the data using Spark Structured Streaming (parse JSON, normalize fields, explode nested data)

- Insert transformed records into the staging table in PostgreSQL

   ![img_13.png](images%2Fimg_13.png)

#### 3. Trigger upsert_dag DAG – this will:

   - Read data from staging table

   - Upsert into the main table
   ![img_12.png](images%2Fimg_12.png)

## ✅ Features
Incremental data crawling based on lastModificationTime

Timezone-safe datetime comparison (all times converted to UTC)

Real-time streaming via Kafka + Spark

PostgreSQL upsert logic for deduplication

Modular codebase for easy maintenance



## 👨‍💻 Author
Diu Nguyen

Data Engineer | Fullstack Developer

📧 nguyenhuongdiu1710@gmail.com

