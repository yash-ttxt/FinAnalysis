package com.example.bankanalysis.ingestion

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import scala.jdk.CollectionConverters._

object DatasetLoader {
  /**
   * Load a dataset from a file path
   * @param spark SparkSession
   * @param path Path to the dataset
   * @param schema Optional schema to apply to the dataset
   * @param format File format
   * @param options Options to pass to the reader
   * @return Dataset[Row]
   */
  protected def load(
    spark: SparkSession,
    path: String,
    schema: Option[StructType],
    format: String = "csv",
    options: Map[String, String] = Map("header" -> "true", "inferSchema" -> "true")
  ): Dataset[Row] = {
    try {
      val reader = spark.read.format(format).options(options)
      schema match {
        case Some(schema) => reader.schema(schema).load(path)
        case None => reader.load(path)
      }
    } catch {
      case e: Exception =>
        throw new Exception(s"Error loading dataset at $path: ${e.getMessage}")
    }
  }

  /**
   * Load the comprehensive banking dataset
   * @param spark SparkSession
   * @return Dataset[Row]
   */
  def loadBankingDataset(spark: SparkSession, path: String): Dataset[Row] = {
    val config = ConfigFactory.load()
    val format = config.getString("spark.fileFormats.defaultFormat")
    val options = config.getObject("spark.options").unwrapped().asInstanceOf[java.util.Map[String, String]].asScala.toMap
    load(spark, path, None, format, options)
  }
}