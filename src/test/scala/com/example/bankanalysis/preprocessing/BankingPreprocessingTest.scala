package com.example.bankanalysis.preprocessing

import com.example.bankanalysis.preprocessing.BankingPreprocessor
import com.example.bankanalysis.ingestion.DatasetLoader
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.Dataset
import utils.SparkSessionProvider

class BankingPreprocessingTest extends AnyFunSuite with BeforeAndAfterAll {

  private val SparkAppName = "BankingPreprocessingTest"
  private val SparkMaster = "local[*]"
  private var spark: SparkSession = _
  private val testingDataPath = "src/test/resources/data/raw/Comprehensive_Banking_Database_Testing.csv"

  override def beforeAll(): Unit = {
    spark = initializeSparkSession()
  }

  override def afterAll(): Unit = {
    stopSparkSession()
  }

  test("testPreprocess") {
    val dataset = DatasetLoader.loadBankingDataset(spark, testingDataPath)
    val preprocessor = BankingPreprocessor
    val processedData = preprocessor.process(dataset)
    assert(processedData.count() == 14)
  }

  private def initializeSparkSession(): SparkSession = {
    SparkSessionProvider.getSparkSession(SparkAppName, SparkMaster)
  }

  private def stopSparkSession(): Unit = {
    spark.stop()
  }
}