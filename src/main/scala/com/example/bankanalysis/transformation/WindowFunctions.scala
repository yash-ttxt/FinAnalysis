package com.example.bankanalysis.transformation

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object WindowFunctions {
  /**
   * This method is responsible for getting the weekly average transaction amount by customer
   * @param df: DataFrame
   * @return DataFrame
   */
  def weeklyAverageTransactionAmountByCustomer(df: DataFrame): DataFrame = {
    val windowSpec = Window.partitionBy("email").orderBy(col("transaction_date").cast("timestamp").cast("Long")).rangeBetween(-7 * 86400, 0)
    df
      .withColumn("transaction_date", to_timestamp(col("transaction_date"), "MM/dd/yyyy"))
      .groupBy(col("email"), window(col("transaction_date"), "7 days").alias("weekly_window"))
      .agg(avg(col("transaction_amount")).as("weekly_avg_transaction_amount"))
      .select(
        col("email"),
        col("weekly_window.start").alias("window_start"),
        col("weekly_window.end").alias("window_end"),
        col("weekly_avg_transaction_amount")
      )
  }

  /**
   * This method is responsible for getting the customer rank by branch on transaction amount
   * @param df: DataFrame
   * @return DataFrame
   */
  def customerRankByBranchOnTransactionAmount(df: DataFrame): DataFrame = {
    val aggregatedDf = df.groupBy("branch_id", "email").agg(sum("transaction_amount").as("total_transaction_amount"))
    val windowSpec = Window.partitionBy("branch_id").orderBy(col("total_transaction_amount").desc)
    aggregatedDf.withColumn("rank", dense_rank().over(windowSpec)).filter(col("rank") === 1).drop("rank")
  }
}
