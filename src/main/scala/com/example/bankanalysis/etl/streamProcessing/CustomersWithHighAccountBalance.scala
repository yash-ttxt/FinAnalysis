package com.example.bankanalysis.etl.streamProcessing

import com.example.bankanalysis.transformation.SQL
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.Trigger

/**
 * This class is responsible for processing the stream of data to get customers with high account balance
 */
class CustomersWithHighAccountBalance extends StreamBase {
  private val transformationConfig: Config = ConfigFactory.load("stream_transformations.conf").getConfig("transformations")

  /**
   * This method is responsible for transforming the stream of data
   * @param df: DataFrame
   * @param spark: SparkSession
   * @return DataFrame
   */
  override protected def transform(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    df.createOrReplaceTempView(etlJobConstants.CUSTOMERS_WITH_HIGH_ACCOUNT_BALANCE)
    SQL.customersWithHighAccountBalance(etlJobConstants.CUSTOMERS_WITH_HIGH_ACCOUNT_BALANCE, transformationConfig.getInt("customersWithHighAccountBalance.threshold"))

  }
}
