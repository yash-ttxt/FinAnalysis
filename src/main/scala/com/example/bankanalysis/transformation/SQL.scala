package com.example.bankanalysis.transformation

import org.apache.spark.sql.{DataFrame, SparkSession}

object SQL {
  def customersWithHighAccountBalance(spark: SparkSession, tableName: String, highAccountBalanceThreshold: Double): DataFrame = {
    spark.sql(s"SELECT * FROM ${tableName} WHERE account_balance > ${highAccountBalanceThreshold}")
  }

  // TBD: Implement the highAccountBalanceThreshold method
  def highAccountBalanceThreshold(spark: SparkSession, tableName: String): Double = {
    spark.sql(s"SELECT AVG(account_balance) as avg_account_balance FROM ${tableName}").first().getDouble(0)
  }

  def transactionWithHighTransactionAmount(spark: SparkSession, tableName: String): DataFrame = {
    spark.sql(s"SELECT a.customer_id, a.email, a.first_name, a.last_name, a.transaction_id, a.transaction_date, a.transaction_amount, b.avg_transaction_amount FROM ${tableName} a JOIN (SELECT email, AVG(transaction_amount) as avg_transaction_amount FROM ${tableName} GROUP BY email) b ON a.email = b.email WHERE a.transaction_amount > 0.1*b.avg_transaction_amount")
  }
}
