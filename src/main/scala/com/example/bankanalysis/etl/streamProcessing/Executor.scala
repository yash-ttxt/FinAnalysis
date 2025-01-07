package com.example.bankanalysis.etl.streamProcessing

import io.github.cdimascio.dotenv.Dotenv
import org.apache.spark.sql.SparkSession
import utils.{Logger, ETLMonitor}
import etlJobConstants._

/**
 * This class is responsible for executing the ETL job
 */
object Executor {
  /**
   * This method is responsible for executing the ETL job
   * @param etlJob: String
   * @param spark: SparkSession
   * @param dotenv: Dotenv
   */
  def main(etlJob: String)(implicit spark: SparkSession, dotenv: Dotenv): Unit = {
    try {
      ETLMonitor.updateJobStatus(etlJob, "STARTED")
      ETLMonitor.updateJobStartTime(etlJob)
      etlJob match {
        case TOP_10_CUSTOMER_BY_TRANSACTION_VOLUME => new Top10CustomerByTransactionVolume().main()
        case MONTHLY_TRANSACTION_VOLUME_BY_BRANCH => new MonthlyTransactionVolumeByBranch().main()
//        case WEEKLY_AVERAGE_TRANSACTION_BY_CUSTOMER => new WeeklyAverageTransactionByCustomer().process()
//        case CUSTOMERS_WITH_HIGH_ACCOUNT_BALANCE => new CustomersWithHighAccountBalance().process()
//        case TRANSACTIONS_WITH_HIGH_TRANSACTION_AMOUNT => new TransactionWithHighTransactionAmount().process()
        case _ => Logger.logError(new Exception("Invalid ETL job"))
      }
      ETLMonitor.updateJobStatus(etlJob, "COMPLETED")
    } catch {
      case e: Exception =>
        e.printStackTrace()
        Logger.logError(e)
        ETLMonitor.updateJobStatus(etlJob, "FAILED")
    }
  }
}
