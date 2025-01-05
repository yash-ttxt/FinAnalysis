package com.example.bankanalysis.transformation

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object WindowFunctions {
  def weeklyAverageTransactionAmountByCustomer(df: DataFrame): DataFrame = {
    val partitionedDf = df.repartition(col("email"))
    val windowSpec = Window.partitionBy("email").orderBy(col("transaction_date").cast("timestamp").cast("Long")).rangeBetween(-7 * 86400, 0)
    partitionedDf
      .withColumn("weekly_avg_transaction_amount", avg(col("transaction_amount")).over(windowSpec))
      .select("email", "transaction_date", "transaction_amount", "weekly_avg_transaction_amount")
  }
}
