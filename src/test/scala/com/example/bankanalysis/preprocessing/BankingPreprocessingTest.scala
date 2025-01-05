package com.example.bankanalysis.preprocessing

import com.example.bankanalysis.BaseTest
import com.example.bankanalysis.ingestion.DatasetLoader
import org.apache.spark.sql.SparkSession
import utils.SparkSessionProvider

class BankingPreprocessingTest extends BaseTest {
  override protected var SparkAppName: String = "BankingPreprocessingTest"
  override protected var SparkMaster: String = "local[*]"
  override protected var spark: SparkSession = _
  private val testingDataPath = "src/test/resources/data/raw/Comprehensive_Banking_Database_Testing.csv"

  test("testPreprocess") {
    val dataset = DatasetLoader.loadBankingDataset(spark, testingDataPath)
    val preprocessor = BankingPreprocessor
    val processedData = preprocessor.process(dataset)
    assert(processedData.count() == 14)
  }
}