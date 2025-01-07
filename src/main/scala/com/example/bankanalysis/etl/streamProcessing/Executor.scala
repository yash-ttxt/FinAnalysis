package com.example.bankanalysis.etl.streamProcessing

import io.github.cdimascio.dotenv.Dotenv
import org.apache.spark.sql.SparkSession
import utils.Logger
import etlJobConstants._

object Executor {
  def main(etlJob: String)(implicit spark: SparkSession, dotenv: Dotenv): Unit = {
    try {
      etlJob match {
        case TOP_10_CUSTOMER_BY_TRANSACTION_VOLUME => new Top10CustomerByTransactionVolume().process()
        case MONTHLY_TRANSACTION_VOLUME_BY_BRANCH => new MonthlyTransactionVolumeByBranch().process()
        case WEEKLY_AVERAGE_TRANSACTION_BY_CUSTOMER => new WeeklyAverageTransactionByCustomer().process()
        case CUSOMTERS_WITH_HIGH_ACCOUNT_BALANCE => new CustomersWithHighAccountBalance().process()
        case TRANSACTIONS_WITH_HIGH_TRANSACTION_AMOUNT => new TransactionWithHighTransactionAmount().process()
        case _ => Logger.logError(new Exception("Invalid ETL job"))
      }

    } catch {
      case e: Exception => e.printStackTrace()
      Logger.logError(e)
    }
  }
}
