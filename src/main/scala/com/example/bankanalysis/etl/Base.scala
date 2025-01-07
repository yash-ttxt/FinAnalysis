package com.example.bankanalysis.etl

import com.example.bankanalysis.ingestion.DatasetLoader
import com.example.bankanalysis.preprocessing.BankingPreprocessor
import com.typesafe.config.{Config, ConfigFactory}
import io.github.cdimascio.dotenv.Dotenv
import org.apache.spark.sql.{DataFrame, SparkSession}
import utils.TransformationsConfiguration
import scala.jdk.CollectionConverters._
import utils.Logger

abstract class Base {
  protected var config: Config

  protected def getDataFrame()(implicit spark: SparkSession, dotenv: Dotenv): DataFrame

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

  protected def getStorageOptions(transformation: String): Map[String, Map[String, String]] = {
    val transformationConfig = config.getConfig(s"transformations.$transformation")
    transformationConfig.root().keySet().toArray.map { format =>
      val formatConfig = transformationConfig.getConfig(format.toString)
      format.toString -> formatConfig.entrySet().asScala.map { entry =>
        entry.getKey -> entry.getValue.unwrapped().toString
      }.toMap
    }.toMap
  }

  protected def transformAndStore(df: DataFrame, transformation: DataFrame => DataFrame, storageOptions: Map[String, Map[String, String]]): Unit

  def main()(implicit spark: SparkSession, dotenv: Dotenv): Unit = {
    try{
      val transformationsAndStorageOptions = getTransformationsConf
      val df: DataFrame = getDataFrame
      transformationsAndStorageOptions.foreach { case (transformation, storageOptions) =>
        try {
          transformAndStore(df, transformation, storageOptions)
        } catch {
          case e: Exception =>
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
