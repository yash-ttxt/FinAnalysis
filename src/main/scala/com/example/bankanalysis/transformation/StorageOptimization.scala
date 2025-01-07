package com.example.bankanalysis.transformation

import org.apache.spark.sql.DataFrame

object StorageOptimization {
  /**
   * This method is responsible for writing the DataFrame to a parquet file
   * @param df: DataFrame
   * @param path: String
   */
  def toParquetPartitioned(df: DataFrame, path: String, partitionColumns: Array[String]): Unit = {
    df.write.partitionBy(partitionColumns: _*).parquet(path)
  }

  /**
   * This method is responsible for writing the DataFrame to a bucketed table
   * @param df: DataFrame
   * @param tableName: String
   * @param bucketColumn: String
   * @param numBuckets: Int
   */
  def toBucketedTable(df: DataFrame, tableName: String, bucketColumn: String, numBuckets: Int): Unit = {
    df.write.bucketBy(numBuckets, bucketColumn).saveAsTable(tableName)
  }

  /**
   * This method is responsible for writing the DataFrame to a delta table
   * @param df: DataFrame
   * @param tableName: String
   */
  def top10TransactionVolumeByCustomerId(df: DataFrame): DataFrame = {
    df.groupBy("customer_id").sum("transaction_amount").orderBy("sum(transaction_amount)").limit(10)
  }
}
