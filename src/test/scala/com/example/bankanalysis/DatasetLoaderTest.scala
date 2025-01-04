package com.example.bankanalysis

import com.example.bankanalysis.ingestion.DatasetLoader
import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll

class DatasetLoaderTest extends AnyFunSuite with BeforeAndAfterAll {

  private val SparkAppName = "DatasetLoaderTest"
  private val SparkMaster = "local[*]"
  private val spark: SparkSession = initializeSparkSession()

  override def beforeAll(): Unit = {
  }

  override def afterAll(): Unit = {
    stopSparkSession()
  }

  test("testLoadBankingDataset") {
    val dataset = DatasetLoader.loadBankingDataset(spark)
    assert(dataset.count() > 0)
  }

  private def initializeSparkSession(): SparkSession = {
    SparkSession.builder
      .appName(SparkAppName)
      .master(SparkMaster)
      .getOrCreate()
  }

  private def stopSparkSession(): Unit = {
    spark.stop()
  }
}
