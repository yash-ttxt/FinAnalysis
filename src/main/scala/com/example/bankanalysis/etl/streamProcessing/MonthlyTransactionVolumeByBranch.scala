package com.example.bankanalysis.etl.streamProcessing

import com.example.bankanalysis.transformation.Aggregations
import com.typesafe.config.{Config, ConfigFactory}
import io.github.cdimascio.dotenv.Dotenv
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * This class is responsible for processing the stream of data to get monthly transaction volume by branch
 */
class MonthlyTransactionVolumeByBranch extends StreamBase {
  private val transformationConfig: Config = ConfigFactory.load("stream_transformations.conf").getConfig("transformations")

  /**
   * This method is responsible for transforming the stream of data
   * @param df: DataFrame
   * @param spark: SparkSession
   * @param dotenv: Dotenv
   * @return DataFrame
   */
  override protected def transform(df: DataFrame)(implicit spark: SparkSession, dotenv: Dotenv): DataFrame = {
    Aggregations.monthlyTransactionVolumeByBranch(df)
  }
}
