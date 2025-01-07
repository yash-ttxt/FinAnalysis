package com.example.bankanalysis.etl.streamProcessing

import com.example.bankanalysis.etl.streamProcessing.StreamBase
import com.typesafe.config.{Config, ConfigFactory}
import com.example.bankanalysis.transformation.Aggregations
import com.typesafe.config.Config
import io.github.cdimascio.dotenv.Dotenv
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.Trigger

/**
 * This class is responsible for processing the stream of data to get top 10 customers by transaction volume
 */
class Top10CustomerByTransactionVolume extends StreamBase {
  private val transformationConfig: Config = ConfigFactory.load("stream_transformations.conf").getConfig("transformations")

  /**
   * This method is responsible for transforming the stream of data
   * @param df: DataFrame
   * @param spark: SparkSession
   * @param dotenv: Dotenv
   * @return DataFrame
   */
  override protected def transform(df: DataFrame)(implicit spark: SparkSession, dotenv: Dotenv): DataFrame = {
    Aggregations.top10TotalTransactionAmountByCustomer(df)
  }
}
