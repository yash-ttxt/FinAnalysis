package com.example.bankanalysis.etl.streamProcessing

import com.example.bankanalysis.transformation.SQL
import com.typesafe.config.{Config, ConfigFactory}
import io.github.cdimascio.dotenv.Dotenv
import org.apache.spark.sql.{DataFrame, SparkSession}

class CustomersWithHighAccountBalance extends StreamBase {
  private val transformationConfig: Config = ConfigFactory.load("stream_transformations.conf").getConfig("transformations")

  override protected def transform(df: DataFrame)(implicit spark: SparkSession, dotenv: Dotenv): DataFrame = {
    df.createOrReplaceTempView(etlJobConstants.CUSOMTERS_WITH_HIGH_ACCOUNT_BALANCE)
    SQL.customersWithHighAccountBalance(etlJobConstants.CUSOMTERS_WITH_HIGH_ACCOUNT_BALANCE, transformationConfig.getInt("customersWithHighAccountBalance.threshold"))

  }

  override protected def writeStream(df: DataFrame)(implicit spark: SparkSession, dotenv: Dotenv): Unit = {
    df.writeStream
      .outputMode("append")
      .format("parquet")
      .option("path", transformationConfig.getString("customersWithHighAccountBalance.parquet.outputPath"))
      .option("checkpointLocation", transformationConfig.getString("customersWithHighAccountBalance.parquet.checkpointLocation"))
      .start()
      .awaitTermination(4 * 60 * 1000) // Todo: Read from config
  }
}
