package com.example.bankanalysis.ingestion

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import utils.SparkSessionProvider

class DatasetLoaderTest extends AnyFunSuite with BeforeAndAfterAll {

  private val SparkAppName = "DatasetLoaderTest"
  private val SparkMaster = "local[*]"
  private var spark: SparkSession = _
  private val testingDataPath = "src/test/resources/data/raw/Comprehensive_Banking_Database_Testing.csv"

  override def beforeAll(): Unit = {
    spark = initializeSparkSession()
  }

  override def afterAll(): Unit = {
    stopSparkSession()
  }

  test("testLoadBankingDataset") {
    val dataset = DatasetLoader.loadBankingDataset(spark, testingDataPath)
    assert(dataset.count() > 0)
  }

  private def initializeSparkSession(): SparkSession = {
    SparkSessionProvider.getSparkSession(SparkAppName, SparkMaster)

  }

  private def stopSparkSession(): Unit = {
    spark.stop()
  }
}
