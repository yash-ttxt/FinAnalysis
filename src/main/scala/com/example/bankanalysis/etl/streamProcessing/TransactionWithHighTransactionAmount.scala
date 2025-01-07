package com.example.bankanalysis.etl.streamProcessing

import com.example.bankanalysis.transformation.SQL
import com.typesafe.config.{Config, ConfigFactory}
import io.github.cdimascio.dotenv.Dotenv
import org.apache.spark.sql.{DataFrame, SparkSession}

class TransactionWithHighTransactionAmount extends StreamBase {
  private val transformationConfig: Config = ConfigFactory.load("stream_transformations.conf").getConfig("transformations")

  override protected def transform(df: DataFrame)(implicit spark: SparkSession, dotenv: Dotenv): DataFrame = {
    df.createOrReplaceTempView(etlJobConstants.TRANSACTIONS_WITH_HIGH_TRANSACTION_AMOUNT)
    SQL.transactionWithHighTransactionAmount(etlJobConstants.TRANSACTIONS_WITH_HIGH_TRANSACTION_AMOUNT)

  }

  override protected def writeStream(df: DataFrame)(implicit spark: SparkSession, dotenv: Dotenv): Unit = {
    df.writeStream
      .outputMode("append")
      .format("parquet")
      .option("path", transformationConfig.getString("transactionWithHighTransactionAmount.parquet.outputPath"))
      .option("checkpointLocation", transformationConfig.getString("transactionWithHighTransactionAmount.parquet.checkpointLocation"))
      .start()
      .awaitTermination(4 * 60 * 1000) // Todo: Read from config
  }
}
