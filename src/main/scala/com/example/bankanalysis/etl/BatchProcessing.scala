package com.example.bankanalysis.etl

import com.example.bankanalysis.ingestion.DatasetLoader
import com.example.bankanalysis.preprocessing.BankingPreprocessor
import com.typesafe.config.{Config, ConfigFactory}
import io.github.cdimascio.dotenv.Dotenv
import org.apache.spark.sql.{DataFrame, SparkSession}
import utils.TransformationsConfiguration
import scala.jdk.CollectionConverters._

class BatchProcessing extends Base {
  override var config: Config = ConfigFactory.load("transformations.conf")

  override protected def getDataFrame()(implicit spark: SparkSession, dotenv: Dotenv): DataFrame = {
    val dataset = DatasetLoader.loadBankingDataset(dotenv.get("RAW_BANKING_DATASET_PATH"))
    BankingPreprocessor.process(dataset)
    dataset.persist()
    dataset
  }

  override protected def transformAndStore(df: DataFrame, transformation: DataFrame => DataFrame, storageOptions: Map[String, Map[String, String]]): Unit = {
    val transformedFrame = transformation(df)
    storageOptions.foreach { case (format, options) =>
      transformedFrame.write
        .format(format)
        .options(options)
        .save()
    }
  }
}
