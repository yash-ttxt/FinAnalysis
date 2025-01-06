package com.example.bankanalysis.etl

import com.typesafe.config.{Config, ConfigFactory}
import io.github.cdimascio.dotenv.Dotenv
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import utils.StreamSchemaProvider.stream_schema
import com.example.bankanalysis.preprocessing.BankingPreprocessor
import com.example.bankanalysis.transformation.WindowFunctions
import org.apache.spark.sql.functions.{col, count, current_timestamp, sum, to_timestamp}
import org.apache.spark.sql.functions.{col, count, sum, to_timestamp}

class StreamProcessing extends Base {
  override var config: Config = ConfigFactory.load("stream_transformations.conf")

  protected def getDataFrame()(implicit spark: SparkSession, dotenv: Dotenv): DataFrame = {
    val applicationConfig = ConfigFactory.load("application.conf")
    val df: DataFrame = spark.readStream
      .format("json")
      .option("path", applicationConfig.getString("spark.stream.path"))
      .option("header", "true")
      .schema(stream_schema())
      .load()

    val processedDf = BankingPreprocessor.process(df)
    processedDf
  }

  override protected def transformAndStore(df: DataFrame, transformation: DataFrame => DataFrame, storageOptions: Map[String, Map[String, String]]): Unit = {
    val transformedDf = transformation(df)
    storageOptions.foreach { case (format, options) =>
      val query: StreamingQuery = transformedDf.writeStream
        .format(format)
        .outputMode(options("outputMode"))
        .option("truncate", "false")
        .trigger(Trigger.ProcessingTime("20 seconds")) // Todo: Read from config
        .start()
      query.awaitTermination(1*60*1000) // Todo: Read from config
    }
  }
}
