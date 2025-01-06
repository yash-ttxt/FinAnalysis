package com.example.bankanalysis.transformation

import org.apache.spark.sql.DataFrame

object StorageOptimization {
  def toParquetPartitioned(df: DataFrame, path: String, partitionColumns: Array[String]): Unit = {
    df.write.partitionBy(partitionColumns: _*).parquet(path)
  }

  def toBucketedTable(df: DataFrame, tableName: String, bucketColumn: String, numBuckets: Int): Unit = {
    df.write.bucketBy(numBuckets, bucketColumn).saveAsTable(tableName)
  }

  def top10TransactionVolumeByCustomerId(df: DataFrame): DataFrame = {
    df.groupBy("customer_id").sum("transaction_amount").orderBy("sum(transaction_amount)").limit(10)
  }
}
