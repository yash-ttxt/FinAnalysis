package com.example.bankanalysis.preprocessing

import com.example.bankanalysis.preprocessing.BasePreprocessor
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{abs, col, isnan, isnull, to_date, udf}

import scala.jdk.CollectionConverters._

object BankingPreprocessor extends BasePreprocessor {
  private val config = ConfigFactory.load()
  override def relevantColumns(): List[String] = config.getStringList("spark.comprehensiveBankingData.relevantColumns").asScala.toList

  override protected def cleanData(df: DataFrame): DataFrame = {
    df.withColumn("Transaction Date", to_date(col("Transaction Date"), "MM/dd/yyyy"))
      .withColumn("Calculated Transaction Amount", abs(col("Account Balance").cast("double") - col("Account Balance After Transaction").cast("double")))
      .na.fill(Map(
        "First Name" -> "Unknown",
        "Last Name" -> "Unknown",
        "Transaction Amount" -> "Calculated Transaction Amount",
        "Branch ID" -> "Branch ID to City"
      ))
  }

  override protected def renameColumns(df: DataFrame): DataFrame = {
    val renameColVals = config.getObject("spark.comprehensiveBankingData.rawColNameToProcessedColName").unwrapped().asInstanceOf[java.util.Map[String, String]].asScala.toMap
    renameColVals.foldLeft(df)((accDf, colName) => accDf.withColumnRenamed(colName._1, colName._2))
  }

  override protected def preprocessData(df: DataFrame): DataFrame = {
    df.withColumn("transaction_amount", abs(col("transaction_amount")))
      .withColumn("transaction_amount", col("transaction_amount").cast("double"))
      .withColumn("age", col("age").cast("integer"))
      .withColumn("account_balance", col("account_balance").cast("double"))
      .withColumn("account_balance_after_transaction", col("account_balance_after_transaction").cast("double"))
      .withColumn("branch_id", col("branch_id").cast("integer"))
  }
}
