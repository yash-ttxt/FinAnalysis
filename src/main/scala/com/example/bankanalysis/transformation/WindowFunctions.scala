package com.example.bankanalysis.transformation

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object WindowFunctions {
  def weeklyAverageTransactionAmountByCustomer(df: DataFrame): DataFrame = {
    val windowSpec = Window.partitionBy("email").orderBy(col("transaction_date").cast("timestamp").cast("Long")).rangeBetween(-7 * 86400, 0)
    df
      .withColumn("weekly_avg_transaction_amount", avg(col("transaction_amount")).over(windowSpec))
      .select("email", "transaction_date", "transaction_amount", "weekly_avg_transaction_amount")
  }

  def customerRankByBranchOnTransactionAmount(df: DataFrame): DataFrame = {
    val aggregatedDf = df.groupBy("branch_id", "email").agg(sum("transaction_amount").as("total_transaction_amount"))
    val windowSpec = Window.partitionBy("branch_id").orderBy(col("total_transaction_amount").desc)
    aggregatedDf.withColumn("rank", dense_rank().over(windowSpec))
  }
}
