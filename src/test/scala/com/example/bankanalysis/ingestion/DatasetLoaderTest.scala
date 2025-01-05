package com.example.bankanalysis.ingestion

import com.example.bankanalysis.BaseTest
import org.apache.spark.sql.SparkSession

class DatasetLoaderTest extends BaseTest {
  override protected var SparkAppName: String = "DatasetLoaderTest"
  override protected var SparkMaster: String = "local[*]"
  override protected var spark: SparkSession = _
  private val testingDataPath = "src/test/resources/data/raw/Comprehensive_Banking_Database_Testing.csv"

  test("testLoadBankingDataset") {
    val dataset = DatasetLoader.loadBankingDataset(spark, testingDataPath)
    assert(dataset.count() > 0)
  }
}
