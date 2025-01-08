package com.example.bankanalysis.etl.streamProcessing

import com.example.bankanalysis.transformation.WindowFunctions
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.Trigger

/**
 * This class is responsible for processing the stream of data to get weekly average transaction amount by customer
 */
class WeeklyAverageTransactionByCustomer extends StreamBase {
  /**
   * This method is responsible for transforming the stream of data
   * @param df: DataFrame
   * @param spark: SparkSession
   * @return DataFrame
   */
  override protected def transform(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    WindowFunctions.weeklyAverageTransactionAmountByCustomer(df)
  }
}
