package com.example.bankanalysis.transformation

import org.apache.spark.sql.{DataFrame, SparkSession}

object SQL {
  /**
   * This method is responsible for getting customers with high account balance
   * @param tableName: String
   * @param highAccountBalanceThreshold: Double
   * @param spark: SparkSession
   * @return DataFrame
   */
  def customersWithHighAccountBalance(tableName: String, highAccountBalanceThreshold: Double)(implicit spark: SparkSession): DataFrame = {
    spark.sql(s"SELECT * FROM $tableName WHERE account_balance > $highAccountBalanceThreshold")
  }

  // TBD: Implement the highAccountBalanceThreshold method
  /**
   * This method is responsible for getting the high account balance threshold
   * @param tableName: String
   * @param spark: SparkSession
   * @return Double
   */
  def highAccountBalanceThreshold(tableName: String)(implicit spark: SparkSession): Double = {
    spark.sql(s"SELECT AVG(account_balance) as avg_account_balance FROM $tableName").first().getDouble(0)
  }

  /**
   * This method is responsible for getting the top 10 customers by transaction volume
   * @param tableName: String
   * @param spark: SparkSession
   * @return DataFrame
   */
  def transactionWithHighTransactionAmount(tableName: String)(implicit spark: SparkSession): DataFrame = {
    spark.sql(s"SELECT a.customer_id, a.email, a.first_name, a.last_name, a.transaction_id, a.transaction_date, a.transaction_amount, b.avg_transaction_amount FROM $tableName a JOIN (SELECT email, AVG(transaction_amount) as avg_transaction_amount FROM $tableName GROUP BY email) b ON a.email = b.email WHERE a.transaction_amount > 0.1*b.avg_transaction_amount")
  }
}
