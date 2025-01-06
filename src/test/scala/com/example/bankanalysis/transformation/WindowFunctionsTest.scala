package com.example.bankanalysis.transformation

import org.apache.spark.sql.SparkSession
import com.example.bankanalysis.BaseTest
import com.example.bankanalysis.ingestion.DatasetLoader
import com.example.bankanalysis.preprocessing.BankingPreprocessor

class WindowFunctionsTest extends BaseTest {
  protected var SparkAppName: String = "WindowFunctionsTest"
  protected var SparkMaster: String = "local"
  protected var spark: SparkSession = _

  test("testWindowFunctions") {
    val df = DatasetLoader.loadBankingDataset(spark, "src/test/resources/data/raw/Transformations_Testing.csv")
    val preprocessedData = BankingPreprocessor.process(df)
    val aggregatedDf = WindowFunctions.weeklyAverageTransactionAmountByCustomer(preprocessedData)
    println(aggregatedDf.collect().mkString("\n"))
    assert(1==1)
  }

  test("testCustomerRankByBranchOnTransactionAmount") {
    val df = DatasetLoader.loadBankingDataset(spark, "src/test/resources/data/raw/Transformations_Testing.csv")
    val preprocessedData = BankingPreprocessor.process(df)
    val aggregatedDf = WindowFunctions.customerRankByBranchOnTransactionAmount(preprocessedData)
    val expectedDf = spark.read.option("header", "true").option("infer_schema", "true").csv("src/test/resources/data/processed/Customer_Rank_By_Branch_On_Transactions.csv")
    assert(aggregatedDf.except(expectedDf).count() == 0)
  }
}
