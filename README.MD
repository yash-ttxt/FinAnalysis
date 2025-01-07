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

     
