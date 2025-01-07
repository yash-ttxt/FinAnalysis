package com.example.bankanalysis.etl.streamProcessing

case object etlJobConstants {
  val TOP_10_CUSTOMER_BY_TRANSACTION_VOLUME = "top10CustomerByTransactionVolume"
  val MONTHLY_TRANSACTION_VOLUME_BY_BRANCH = "monthlyTransactionVolumeByBranch"
  val WEEKLY_AVERAGE_TRANSACTION_BY_CUSTOMER = "weeklyAverageTransactionAmountByCustomer"
  val CUSOMTERS_WITH_HIGH_ACCOUNT_BALANCE = "customersWithHighAccountBalance"
  val TRANSACTIONS_WITH_HIGH_TRANSACTION_AMOUNT = "transactionsWithHighTransactionAmount"
}