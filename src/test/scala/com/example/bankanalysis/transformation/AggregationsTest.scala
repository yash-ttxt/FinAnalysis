package com.example.bankanalysis.transformation

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import utils.SparkSessionProvider
import com.example.bankanalysis.ingestion.DatasetLoader
import com.example.bankanalysis.preprocessing.BankingPreprocessor

class AggregationsTest extends AnyFunSuite with BeforeAndAfterAll {
  private val SparkAppName = "AggregationsTest"
  private val SparkMaster = "local[*]"
  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = initializeSparkSession()
  }

  override def afterAll(): Unit = {
    stopSparkSession()
  }

  test("testTop10TotalTransactionAmountByCustomer") {
    val dataset = DatasetLoader.loadBankingDataset(spark, "src/test/resources/data/raw/Aggregations_Testing.csv")
    val preprocessedData = BankingPreprocessor.process(dataset)
    val aggregatedData = Aggregations.top10TotalTransactionAmountByCustomer(preprocessedData)
//    println(aggregatedData.collect().mkString("\n"))
    val expectedDataset = spark.read.option("header", "true").csv("src/test/resources/data/processed/Total_Transaction_Amount_By_Customer.csv")
    assert(aggregatedData.except(expectedDataset).count() == 0)
  }

  test("testmonthlyTransactionVolumeByBranch") {
    val dataset = DatasetLoader.loadBankingDataset(spark, "src/test/resources/data/raw/Aggregations_Testing.csv")
    val preprocessedData = BankingPreprocessor.process(dataset)
    val aggregatedData = Aggregations.monthlyTransactionVolumeByBranch(preprocessedData)
//    println(aggregatedData.collect().mkString("\n"))
    val expectedDataset = spark.read.option("header", "true").csv("src/test/resources/data/processed/Monthly_Transaction_Volume_Branch.csv")
    assert(aggregatedData.except(expectedDataset).count() == 0)
  }

  private def initializeSparkSession(): SparkSession = {
    SparkSessionProvider.getSparkSession(SparkAppName, SparkMaster)
  }

  private def stopSparkSession(): Unit = {
    spark.stop()
  }
}
