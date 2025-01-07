package com.example.bankanalysis.etl.streamProcessing

import com.example.bankanalysis.transformation.Aggregations
import com.typesafe.config.{Config, ConfigFactory}
import io.github.cdimascio.dotenv.Dotenv
import org.apache.spark.sql.{DataFrame, SparkSession}

class MonthlyTransactionVolumeByBranch extends StreamBase {
  private val transformationConfig: Config = ConfigFactory.load("stream_transformations.conf").getConfig("transformations")

  override protected def transform(df: DataFrame)(implicit spark: SparkSession, dotenv: Dotenv): DataFrame = {
    Aggregations.monthlyTransactionVolumeByBranch(df)
  }

  override protected def writeStream(df: DataFrame)(implicit spark: SparkSession, dotenv: Dotenv): Unit = {
    df.writeStream
      .outputMode("append")
      .format("parquet")
      .option("path", transformationConfig.getString("monthlyTransactionVolumeByBranch.parquet.outputPath"))
      .option("checkpointLocation", transformationConfig.getString("monthlyTransactionVolumeByBranch.parquet.checkpointLocation"))
      .start()
      .awaitTermination(4 * 60 * 1000) // Todo: Read from config
  }
}
