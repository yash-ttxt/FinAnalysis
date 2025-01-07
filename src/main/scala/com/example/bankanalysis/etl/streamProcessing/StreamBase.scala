package com.example.bankanalysis.etl.streamProcessing

import com.typesafe.config.{Config, ConfigFactory}
import io.github.cdimascio.dotenv.Dotenv
import org.apache.spark.sql.{DataFrame, SparkSession}
import utils.StreamSchemaProvider.stream_schema
import com.example.bankanalysis.preprocessing.BankingPreprocessor

abstract class StreamBase {
  protected def getDataFrame()(implicit spark: SparkSession, dotenv: Dotenv): DataFrame = {
    val applicationConfig: Config = ConfigFactory.load("application.conf")
    val df: DataFrame = spark.readStream
      .format("json")
      .option("path", applicationConfig.getString("spark.stream.path"))
      .option("header", "true")
      .schema(stream_schema())
      .load()

    val processedDf = BankingPreprocessor.process(df)
    processedDf
  }

  protected def transform(df: DataFrame)(implicit spark: SparkSession, dotenv: Dotenv): DataFrame

  protected def writeStream(df: DataFrame)(implicit spark: SparkSession, dotenv: Dotenv): Unit

  def process()(implicit spark: SparkSession, dotenv: Dotenv): Unit = {
    val processedDf = getDataFrame()
    val transformedStream = transform(processedDf)
    writeStream(transformedStream)
  }
}
