package com.example.bankanalysis.etl.streamProcessing

import io.github.cdimascio.dotenv.Dotenv
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
   * @param dotenv: Dotenv
   * @return DataFrame
   */
  override protected def transform(df: DataFrame)(implicit spark: SparkSession, dotenv: Dotenv): DataFrame = {
    WindowFunctions.weeklyAverageTransactionAmountByCustomer(df)
  }
}
