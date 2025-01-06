package com.example.bankanalysis.transformation

import com.example.bankanalysis.BaseTest
import org.apache.spark.sql.SparkSession
import com.example.bankanalysis.ingestion.DatasetLoader
import com.example.bankanalysis.preprocessing.BankingPreprocessor

class SQLTest extends BaseTest {
  protected var SparkAppName: String = "SQLTest"
  protected var SparkMaster: String = "local"
  protected var spark: SparkSession = _
  private val tableName = "SQLTest"

  before {
    val df = DatasetLoader.loadBankingDataset(spark, "src/test/resources/data/raw/Transformations_Testing.csv")
    val preprocessedDf = BankingPreprocessor.process(df)
    preprocessedDf.createOrReplaceTempView(tableName)
  }

  test("customersWithHighAccountBalance") {
    val highAccountBalanceThreshold = 7000.0
    val result = SQL.customersWithHighAccountBalance(spark, tableName, highAccountBalanceThreshold)
//    result.write.option("header", "true").csv("src/test/resources/data/processed/Customers_With_High_Account_Balance")
//    print("customersWithHighAccountBalance")
//    println(result.collect())
    val expectedDf = spark.read.option("header", "true").option("inferSchema", "true").csv("src/test/resources/data/processed/Customers_With_High_Account_Balance")
    val cols = result.columns
    assert(result.select(cols.head, cols.tail: _*).except(expectedDf.select(cols.head, cols.tail: _*)).count() == 0)
  }

  test("highAccountBalanceThreshold") {
    val result = SQL.highAccountBalanceThreshold(spark, tableName)
    print("highAccountBalanceThreshold")
//    println(result)
    assert(result == 5642.067142857144)
  }

  test("transactionWithHighTransactionAmount") {
    val result = SQL.transactionWithHighTransactionAmount(spark, tableName)
//    result.write.option("header", "true").csv("src/test/resources/data/processed/Transaction_With_High_Transaction_Amount")
//    print("transactionWithHighTransactionAmount")
//    println(result.collect())
    val expectedDf = spark.read.option("header", "true").option("inferSchema", "true").csv("src/test/resources/data/processed/Transaction_With_High_Transaction_Amount")
    val cols = result.columns
    assert(result.select(cols.head, cols.tail: _*).except(expectedDf.select(cols.head, cols.tail: _*)).count() == 0)
  }
}
