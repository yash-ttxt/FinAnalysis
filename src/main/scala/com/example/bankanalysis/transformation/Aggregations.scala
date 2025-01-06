package com.example.bankanalysis.transformation

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row

object Aggregations {

  def top10TotalTransactionAmountByCustomer(df: DataFrame): DataFrame = {
    df.groupBy(col("email"))
      .agg(sum(col("transaction_amount"))
        .as("total_transaction_amount"))
      .select("email", "total_transaction_amount")
      .orderBy(col("total_transaction_amount").desc)
      .limit(10)
  }

  def monthlyTransactionVolumeByBranch(df: DataFrame): DataFrame = {
    val newDf = df.withColumn("month", month(col("transaction_date")))
    newDf.groupBy(col("month"), col("branch_id"))
      .agg(sum(col("transaction_amount"))
        .as("transaction_volume"))
      .select("month", "branch_id", "transaction_volume")
  }
}
