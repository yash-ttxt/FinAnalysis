package com.example.bankanalysis.etl.streamProcessing

import com.typesafe.config.{Config, ConfigFactory}
import io.github.cdimascio.dotenv.Dotenv
import org.apache.spark.sql.{DataFrame, SparkSession}
import utils.StreamSchemaProvider.stream_schema
import com.example.bankanalysis.preprocessing.BankingPreprocessor
import org.apache.spark.sql.streaming.Trigger

/**
 * This class is responsible for processing the stream of data
 *
 * **************************************************************************
 * A similar concept used in Batch Processing using config driven architecture
 * was tried to use here but due to complications in the different sink options and
 * being able to handle all sink options this is TBD.
 * **************************************************************************
 */
abstract class StreamBase {
  /**
   * This method is responsible for reading the stream of data
   * @param spark: SparkSession
   * @param dotenv: Dotenv
   * @return DataFrame
   */
  protected def getDataFrame()(implicit spark: SparkSession, dotenv: Dotenv): DataFrame = {
    val applicationConfig: Config = ConfigFactory.load("application.conf")
    val df: DataFrame = spark.readStream
      .format("json")
      .option("path", applicationConfig.getString("spark.stream.path"))
      .option("header", "true")
      .schema(stream_schema)
      .load()

    val processedDf = BankingPreprocessor.process(df)
    processedDf
  }

  /**
   * This method is responsible for transforming the stream of data
   * @param df: DataFrame
   * @param spark: SparkSession
   * @param dotenv: Dotenv
   * @return DataFrame
   */
  protected def transform(df: DataFrame)(implicit spark: SparkSession, dotenv: Dotenv): DataFrame

  /**
   * This method is responsible for writing the stream of data
   * @param df: DataFrame
   * @param spark: SparkSession
   * @param dotenv: Dotenv
   */
  protected def writeStream(df: DataFrame)(implicit spark: SparkSession, dotenv: Dotenv): Unit = {
    df.writeStream
      .outputMode("complete")
      .format("console")
      .trigger(Trigger.ProcessingTime("20 seconds"))
      .start()
      .awaitTermination(1 * 1 * 1000) // Todo: Read from config
  }

  /**
   * This method is responsible for processing the stream of data
   * @param spark: SparkSession
   * @param dotenv: Dotenv
   */
  def main()(implicit spark: SparkSession, dotenv: Dotenv): Unit = {
    val processedDf = getDataFrame()
    val transformedStream = transform(processedDf)
    writeStream(transformedStream)
  }
}
