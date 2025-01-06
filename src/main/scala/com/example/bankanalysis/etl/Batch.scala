package com.example.bankanalysis.etl

import com.example.bankanalysis.ingestion.DatasetLoader
import com.example.bankanalysis.preprocessing.BankingPreprocessor
import com.typesafe.config.{Config, ConfigFactory}
import io.github.cdimascio.dotenv.Dotenv
import org.apache.spark.sql.{DataFrame, SparkSession}
import utils.TransformationsConfiguration

import java.time.LocalDate
import java.time.format.DateTimeFormatter

object Batch {
  protected var spark: SparkSession = _
  private var dotEnv: Dotenv = _
  protected var config: Config = _

  private def getDataFrame: DataFrame = {
    val dataset = DatasetLoader.loadBankingDataset(spark, dotEnv.get("RAW_BANKING_DATASET_PATH"))
    BankingPreprocessor.process(dataset)
  }

  private def getTransformationsConf: Map[DataFrame => DataFrame, Map[String, Map[String, String]]] = {
    this.config = ConfigFactory.load("transformations.conf")
    val transformationsConfig = config.getConfig("transformations")
    TransformationsConfiguration.methods.flatMap { case (name, method) =>
      if (transformationsConfig.hasPath(name)) {
        Some(method -> getStorageOptions(name))
      } else {
        None
      }
    }
  }

  private def getStorageOptions(transformation: String): Map[String, Map[String, String]] = {
    val transformationConfig = config.getConfig(s"transformations.$transformation")
    transformationConfig.root().keySet().toArray.map { format =>
      val formatConfig = transformationConfig.getConfig(format.toString)
      format.toString -> Map(
        "mode" -> formatConfig.getString("mode"),
        "path" -> formatConfig.getString("path")
      )
    }.toMap
  }

  def main(spark: SparkSession): Unit = {
    this.spark = spark

    val transformationsAndStorageOptions = getTransformationsConf
    val df: DataFrame = getDataFrame
    df.persist()
    transformationsAndStorageOptions.foreach { case (transformation, storageOptions) =>
      println(s"Processing transformation: ${transformation.toString}")
      try {
        val transformedDf = transformation(df)
        storageOptions.foreach { case (format, options) =>
          transformedDf.write.format(format).options(options).save()
        }
      } catch {
        case e: Exception =>
          println(s"Error processing transformation: ${e.getMessage}")
      }
    }
  }
}
