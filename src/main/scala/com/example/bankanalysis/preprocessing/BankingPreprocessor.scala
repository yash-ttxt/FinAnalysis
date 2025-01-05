package com.example.bankanalysis.preprocessing

import com.example.bankanalysis.preprocessing.BasePreprocessor
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{abs, col, isnan, isnull, to_date, udf}

import scala.jdk.CollectionConverters._

object BankingPreprocessor extends BasePreprocessor {
  private val config = ConfigFactory.load()
  override def relevantColumns(): List[String] = config.getStringList("spark.relevantColumns.comprehensiveBankingData").asScala.toList

  override def cleanData(df: DataFrame): DataFrame = {
    val cityToBranchID = df.filter(col("City").isNotNull && col("Branch ID").isNotNull)
      .select("City", "Branch ID")
      .distinct()
      .collect()
      .map(row => row.getString(0) -> row.getInt(1))
      .toMap

    val getBranchID = udf((city: String) => cityToBranchID.getOrElse(city, -1))

    df.withColumn("Transaction Date", to_date(col("Transaction Date"), "MM/dd/yyyy"))
      .withColumn("Calculated Transaction Amount", abs(col("Account Balance").cast("double") - col("Account Balance After Transaction").cast("double")))
      .withColumn("Branch ID from City", getBranchID(col("City")))
      .na.fill(Map(
        "First Name" -> "Unknown",
        "Last Name" -> "Unknown",
        "Transaction Amount" -> "Calculated Transaction Amount",
        "Branch ID" -> "Branch ID to City"
      ))
  }

  override def preprocessData(df: DataFrame): DataFrame = {
    df.withColumn("Transaction Amount", abs(col("Transaction Amount")))
      .withColumn("Transaction Amount", col("Transaction Amount").cast("double"))
      .withColumn("Age", col("Age").cast("integer"))
      .withColumn("Account Balance", col("Account Balance").cast("double"))
      .withColumn("Account Balance After Transaction", col("Account Balance After Transaction").cast("double"))
      .withColumn("Branch ID", col("Branch ID").cast("integer"))
  }
}
