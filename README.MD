# Banking Data Analysis
## Introduction
This project is about analyzing the banking data of a bank. The data is in the form of a CSV file. The data can be found in `data/raw/Comprehensive_Banking_Database.csv`.

The aim of this project is to explore and understand the concenpts of Data Engineering specifically from Spark and using Scala. The concepts that we used in this project are:
1. Reading the data from a CSV file.
2. Creating a DataFrame.
3. Performing operations on the DataFrame:
    - Filtering the data.
    - Grouping the data.
    - Aggregating the data.
    - Joining the data.
    - Sorting the data.
    - Bucketing
    - Window Transformations
    - SQL Queries
4. We processed both `BATCH` and `STREAMING` data.
5. We used parquet as the storage option for the `BATCH` data.

To go through the overall design of the project.
1. The main file is `src/main/scala/main.Scala` which is the entry point of the project expects 2 arguments:
    - The first argument is the process type which can be `BATCH` or `STREAM`.
    - If the process type is `STREAM` the second argument is the job to be performed on the streaming data.
2. The Batch process is defined in `src/main/scala/com/example/bankanalysis/etl/BatchProcessing` is designed in a way where `src/main/resources/transformations.conf` can be used to control the execution of the batch process. transformations.conf is a configuration file that contains the list of transformations that need to be performed on the data and also the storage options.
3. To perform Stream Processing a stream simulation script is also included at `src/main/utils/SimulateStream.scala`. This script simulates a stream of 5 rows per second using the same raw data we use for batch processing.
4. The repository also includes test cases for all the transformations and preprocessing that is performed on the data.

## How to run the project
1. Clone the repository.
2. Create a `.env` file as a copy of `.env.example` and update the values.
3. To run the batch process:
   1. Configure the `src/main/resources/transformations.conf` file to define the transformation and storage options.
   2. Run `sbt run BATCH` to run the batch process.
4. To run the stream process:
   1. First we need to simulate the stream:
      1. Run `sbt run` and when follow the instructions to run the stream simulation.
   2. Run `sbt run STREAM <job>` to run the stream process. The value for job can be found in `src/main/scala/com/example/bankanalysis/etl/streamProcessing/etlJobConstants`
   3. Currently only console output for stream processing is supported. But a similar concept of system design used in batch processing will be used in the future.

     
## Tasks

1. Section 1: Data Ingestion and Preprocessing
   1. Code in `src/main/scala/com/example/bankanalysis/ingestion` & `src/main/scala/com/example/bankanalysis/preprocessing`
2. Section 2: Advanced Spark Operations
   1. Aggregations: Code in `src/main/scala/com/example/bankanalysis/transformations/Aggregations.scala`
   2. Window Functions: Code in `src/main/scala/com/example/bankanalysis/transformations/WindowFunctions.scala`
   3. Partitioning and Storage Optimization: Code in `src/main/scala/com/example/bankanalysis/transformations/StorageOptimization.scala`
   4. Spark SQL: Code in `src/main/scala/com/example/bankanalysis/transformations/SQL.scala`
3. Section 3: ETL Pipeline Design
   1. Pipeline Design: Overall Code available in `src/main/scala/com/example/bankanalysis/etl/BatchProcessing.scala`
   2. Stream Data Ingestion: Simulation code in `src/main/utils/SimulateStream.scala` and processing code in `src/main/scala/com/example/bankanalysis/etl/streamProcessing`
   3. Error Handling and Monitoring: Code in `src/main/scala/utils/Logger.scala` and `src/main/scala/utils/ETLMonitor.scala`
   4. Scalability and Automation:
      1. Recommendations for Scalability:
         To ensure scalable and efficient ETL processes, use cloud storage solutions like AWS S3 or Azure Blob for fault-tolerant and distributed data handling, and adopt distributed computing frameworks like Apache Spark. Optimize storage with columnar file formats like Parquet or ORC and enhance performance using partitioning and bucketing based on query patterns. Enable auto-scaling to dynamically adjust resources, and cache frequently accessed data using Spark's in-memory capabilities. Leverage Spark's Catalyst optimizer and Tungsten engine for query optimization, fine-tuning shuffle partitions, joins, and memory. For streaming, parallelize ingestion and optimize latency with structured streaming, stateful operations, and watermarks.
      2. Recommendations for Automation:
         To automate ETL pipelines, use workflow orchestration tools like Apache Airflow, Azure Data Factory, or Google Cloud Composer for scheduling and managing workflows. For simpler setups, use cron jobs for periodic scheduling or adopt event-driven triggers with tools like AWS Lambda for real-time processing. Parameterize configurations to adapt pipelines dynamically. Monitor performance and set up real-time alerts using Prometheus, Grafana, or cloud monitoring solutions. Integrate with CI/CD tools like Jenkins or GitHub Actions for seamless testing and deployment.
4. Section 4: Dashoard Design & Visualization
   1. Code for interactive dashboard is present in `visualization/app.py`. Use `pip install -r visualization/requirements.txt` to install the required libraries.
5. Section 5: Bonus Tasks
   1. Data Quality Checks: Code in `src/main/scala/com/example/bankanalysis/ingestion/DataQualityCheck.scala`
   2. Run using `sbt run` -> select `DataQualityCheck` to run the data quality checks.
        