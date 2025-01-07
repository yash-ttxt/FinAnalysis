package com.example.bankanalysis.etl

import com.example.bankanalysis.ingestion.DatasetLoader
import com.example.bankanalysis.preprocessing.BankingPreprocessor
import com.typesafe.config.{Config, ConfigFactory}
import io.github.cdimascio.dotenv.Dotenv
import org.apache.spark.sql.{DataFrame, SparkSession}
import utils.{Logger, TransformationsConfiguration, ETLMonitor}

import scala.jdk.CollectionConverters._

/**
 * This class is responsible for processing the batch of data.
 * ****************************************************************
 * This class utilizes the concept of Configuration Driven Design
 * to process the data. The configuration provides information about
 * the job to run, where to store the results and also the information
 * about the storage options.
 * ****************************************************************
 */
object BatchProcessing{
  protected val config: Config = ConfigFactory.load("transformations.conf")

  /**
   * This method is responsible for getting the data
   * @param spark: SparkSession
   * @param dotenv: Dotenv
   * @return DataFrame
   */
  protected def getDataFrame()(implicit spark: SparkSession, dotenv: Dotenv): DataFrame = {
    // Todo: Add logic here to load data using current date. But will add this later as when testing creates an unnecessary hassle.
    //    val currentDate = java.time.LocalDate.now().toString
    //    val dataset = DatasetLoader.loadBankingDataset(dotenv.get("RAW_BANKING_DATASET_BASE_PATH")+s"_$currentDate.csv")
    val dataset = DatasetLoader.loadBankingDataset(dotenv.get("RAW_BANKING_DATASET_PATH"))
    BankingPreprocessor.process(dataset)
    dataset.persist()
    dataset
  }

  /**
   * This method is responsible for getting the transformations configuration
   * @return Map[DataFrame => DataFrame, Map[String, Map[String, String]]]
   */
  protected def getTransformationsConf: Map[DataFrame => DataFrame, Map[String, Map[String, String]]] = {
    val transformationsConfig = config.getConfig("transformations")
    TransformationsConfiguration.methods.flatMap { case (name, method) =>
      if (transformationsConfig.hasPath(name)) {
        Some(method -> getStorageOptions(name))
      } else {
        None
      }
    }
  }

  /**
   * This method is responsible for getting the storage options
   * @param transformation: String
   * @return Map[String, Map[String, String]]
   */
  protected def getStorageOptions(transformation: String): Map[String, Map[String, String]] = {
    val transformationConfig = config.getConfig(s"transformations.$transformation")
    transformationConfig.root().keySet().toArray.map { format =>
      val formatConfig = transformationConfig.getConfig(format.toString)
      format.toString -> formatConfig.entrySet().asScala.map { entry =>
        entry.getKey -> entry.getValue.unwrapped().toString
      }.toMap
    }.toMap
  }

  /**
   * This method is responsible for transforming and storing the data
   * @param df: DataFrame
   * @param transformation: DataFrame => DataFrame
   * @param storageOptions: Map[String, Map[String, String]]
   */
  protected def transformAndStore(df: DataFrame, transformation: DataFrame => DataFrame, storageOptions: Map[String, Map[String, String]]): Unit = {
    val transformedFrame = transformation(df)
    storageOptions.foreach { case (format, options) =>
      transformedFrame.write
        .format(format)
        .options(options)
        .save()
    }
  }

  /**
   * This method is responsible for processing the batch of data
   * @param spark: SparkSession
   * @param dotenv: Dotenv
   */
  def main()(implicit spark: SparkSession, dotenv: Dotenv): Unit = {
    try{
      val transformationsAndStorageOptions = getTransformationsConf
      val df: DataFrame = getDataFrame
      transformationsAndStorageOptions.foreach { case (transformation, storageOptions) =>
        try {
          ETLMonitor.updateJobStatus(TransformationsConfiguration.names(transformation), "STARTED")
          transformAndStore(df, transformation, storageOptions)
          ETLMonitor.updateJobStatus(TransformationsConfiguration.names(transformation), "COMPLETED")
        } catch {
          case e: Exception =>
            ETLMonitor.updateJobStatus(TransformationsConfiguration.names(transformation), "FAILED")
            println(s"Error processing transformation: ${e.getMessage}")
            println(e.getStackTrace.mkString("\n"))
            Logger.logError(e, Option(Array(transformation, storageOptions).mkString(":::")))
        }
      }
    } catch {
      case e: Exception =>
        println(s"Error getting data: ${e.getMessage}")
        println(e.getStackTrace.mkString("\n"))
        Logger.logError(e)
    }
  }

}
