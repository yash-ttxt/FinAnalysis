package com.example.bankanalysis.etl.streamProcessing

import com.example.bankanalysis.transformation.SQL
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * This class is responsible for processing the stream of data to get transactions with high transaction amount
 */
class TransactionWithHighTransactionAmount extends StreamBase {
  private val transformationConfig: Config = ConfigFactory.load("stream_transformations.conf").getConfig("transformations")

  /**
   * This method is responsible for transforming the stream of data
   * @param df: DataFrame
   * @param spark: SparkSession
   * @return DataFrame
   */
  override protected def transform(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    df.createOrReplaceTempView(etlJobConstants.TRANSACTIONS_WITH_HIGH_TRANSACTION_AMOUNT)
    SQL.transactionWithHighTransactionAmount(etlJobConstants.TRANSACTIONS_WITH_HIGH_TRANSACTION_AMOUNT)

  }
}
