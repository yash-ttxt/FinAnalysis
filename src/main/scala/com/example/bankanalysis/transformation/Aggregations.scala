package com.example.bankanalysis.transformation

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row

object Aggregations {

  /**
   * This method is responsible for getting the top 10 customers by transaction volume
   * @param df: DataFrame
   * @return DataFrame
   */
  def top10TotalTransactionAmountByCustomer(df: DataFrame): DataFrame = {
    df.groupBy(col("email"))
      .agg(sum(col("transaction_amount"))
        .as("total_transaction_amount"))
      .select("email", "total_transaction_amount")
      .orderBy(col("total_transaction_amount").desc)
      .limit(10)
  }

  /**
   * This method is responsible for getting the monthly transaction volume by branch
   * @param df: DataFrame
   * @return DataFrame
   */
  def monthlyTransactionVolumeByBranch(df: DataFrame): DataFrame = {
    val newDf = df.withColumn("month", month(col("transaction_date")))
    newDf.groupBy(col("month"), col("branch_id"))
      .agg(sum(col("transaction_amount"))
        .as("transaction_volume"))
      .select("month", "branch_id", "transaction_volume")
  }
}
