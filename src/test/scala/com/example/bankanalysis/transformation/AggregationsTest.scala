package com.example.bankanalysis.transformation

import com.example.bankanalysis.BaseTest
import org.apache.spark.sql.SparkSession
import com.example.bankanalysis.ingestion.DatasetLoader
import com.example.bankanalysis.preprocessing.BankingPreprocessor

class AggregationsTest extends BaseTest {

  override protected var SparkAppName: String = "AggregationsTest"
  override protected var SparkMaster: String = "local[*]"
  override protected var spark: SparkSession = _

  test("testTop10TotalTransactionAmountByCustomer") {
    val dataset = DatasetLoader.loadBankingDataset("src/test/resources/data/raw/Transformations_Testing.csv")(spark)
    val preprocessedData = BankingPreprocessor.process(dataset)
    val aggregatedData = Aggregations.top10TotalTransactionAmountByCustomer(preprocessedData)
//    println(aggregatedData.collect().mkString("\n"))
    val expectedDataset = spark.read.option("header", "true").csv("src/test/resources/data/processed/Total_Transaction_Amount_By_Customer.csv")
    assert(aggregatedData.except(expectedDataset).count() == 0)
  }

  test("testMonthlyTransactionVolumeByBranch") {
    val dataset = DatasetLoader.loadBankingDataset("src/test/resources/data/raw/Transformations_Testing.csv")(spark)
    val preprocessedData = BankingPreprocessor.process(dataset)
    val aggregatedData = Aggregations.monthlyTransactionVolumeByBranch(preprocessedData)
//    println(aggregatedData.collect().mkString("\n"))
    val expectedDataset = spark.read.option("header", "true").csv("src/test/resources/data/processed/Monthly_Transaction_Volume_Branch.csv")
    assert(aggregatedData.except(expectedDataset).count() == 0)
  }

}
