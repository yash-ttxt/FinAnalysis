package com.example.bankanalysis.ingestion

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import io.github.cdimascio.dotenv.Dotenv
import utils.{BankigDataSchemaProvider, SparkSessionProvider}

object DataQualityCheck {
  var reportCols: Array[String] = Array()

  def main(args: Array[String]): Unit = {
    val dotenv = Dotenv.load()
    val spark: SparkSession = SparkSessionProvider.getSparkSession("Data Quality Check", "local[*]")
    val df = spark.read.option("header", "true").option("inferSchema", "true").csv(dotenv.get("RAW_BANKING_DATASET_PATH"))
    val rowCount = df.count()

    val qualityChecks = performQualityChecks(df)

    val reportDf = generateReport(qualityChecks)
    reportDf.cache()

    val (checksInvalidated, validatedChecks) = reportCols.partition(colName => reportDf.first().getAs[Long](colName) < rowCount)
    val checkInvalidatedDf = reportDf.select(checksInvalidated.map(col): _*)
    val checkValidatedDf = reportDf.select(validatedChecks.map(col): _*)

    checkInvalidatedDf.show()
    checkValidatedDf.show()

    spark.stop()
  }

  def performQualityChecks(df: DataFrame): DataFrame = {

    val nullCheckDf = isNullCheck(df)
    val dateValidationCheckDf = dateValidationCheck(nullCheckDf)
    val nounLengthCheckDf = nounLengthCheck(dateValidationCheckDf)
    reportCols = reportCols :+ "age is > 14 and < 100"
    val ageCheckDf = nounLengthCheckDf.withColumn("age is > 14 and < 100", col("age").cast("int").gt(14).and(col("age").cast("int").lt(100)))
    reportCols = reportCols :+ "gender is male/female/other"
    val genderCheckDf = ageCheckDf.withColumn("gender is male/female/other", lower(col("Gender").cast("string")).isin("male", "female", "other"))
    reportCols = reportCols :+ "contact number is 11 digits"
    val contactNumberCheckDf = genderCheckDf.withColumn("contact number is 11 digits", length(col("Contact Number")).equalTo(11))
    reportCols = reportCols :+ "email regex check"
    val emailCheckDf = contactNumberCheckDf.withColumn("email regex check", col("Email").rlike("^[A-Za-z0-9+_.-]+@[A-Za-z0-9.-]+$"))
    reportCols = reportCols :+ "account type in savings/current"
    val accountTypeCheckDf = emailCheckDf.withColumn("account type in savings/current", lower(col("Account Type")).isin("savings", "current"))
    reportCols = reportCols :+ "account balance not absurd high"
    val accountBalanceCheckDf = accountTypeCheckDf.withColumn("account balance not absurd high", col("Account Balance").cast("double").lt(10e30))
    reportCols = reportCols :+ "transaction type in withdrawal/deposit/transfer"
    val transactionTypeCheckDf = accountBalanceCheckDf.withColumn("transaction type in withdrawal/deposit/transfer", lower(col("Transaction Type")).isin("withdrawal", "deposit", "transfer"))
    reportCols = reportCols :+ "transaction amount not absurd high"
    val transactionAmountCheckDf = transactionTypeCheckDf.withColumn("transaction amount not absurd high", col("Transaction Amount").cast("double").lt(10e30).and(col("Transaction Amount").cast("double").gt(0)))
    reportCols = reportCols :+ "account balance after transaction is valid"
    val accountBalanceAfterTransactionCheckDf: DataFrame = accountBalanceAfterTransactionCheck(transactionAmountCheckDf)
    reportCols = reportCols :+ "loan amount not absurd high"
    val loanAmountCheckDf = accountBalanceAfterTransactionCheckDf.withColumn("loan amount not absurd high", col("Loan Amount").cast("double").lt(10e30))
    reportCols = reportCols :+ "loan type in personal/auto/mortgage"
    val loanTypeCheckDf = loanAmountCheckDf.withColumn("loan type in personal/auto/mortgage", lower(col("Loan Type")).isin("personal", "auto", "mortgage"))
    reportCols = reportCols :+ "interest rate < 100"
    val interestRateCheckDf = loanTypeCheckDf.withColumn("interest rate < 100", col("Interest Rate").cast("double").lt(100))
    reportCols = reportCols :+ "loan term >= 0"
    val loanTermCheckDf = interestRateCheckDf.withColumn("loan term >= 0", col("Loan Term").cast("int") >= 0)
    reportCols = reportCols :+ "credit limit > 0 and < 10e30"
    val creditLimitCheckDf = loanTermCheckDf.withColumn("credit limit > 0 and < 10e30", col("Credit Limit").cast("double").lt(10e30).and(col("Credit Limit").cast("double").gt(0)))
    reportCols = reportCols :+ "credit card balance < 10e30"
    val creditCardBalanceCheckDf = creditLimitCheckDf.withColumn("credit card balance < 10e30", col("Credit Card Balance").cast("double").lt(lit(10e30)))
    reportCols = reportCols :+ "rewards_points_non_negative"
    val rewardsPointsCheckDf = creditCardBalanceCheckDf.withColumn("rewards_points_non_negative", col("Rewards Points").cast("int") >= 0)
    reportCols = reportCols :+ "feedback type in complaint/suggestion/praise"
    val feedbackTypeCheckDf = rewardsPointsCheckDf.withColumn("feedback type in complaint/suggestion/praise", lower(col("Feedback Type")).isin("complaint", "suggestion", "praise"))
    reportCols = reportCols :+ "resolution status in resolved/pending"
    val resolutionStatusCheckDf = feedbackTypeCheckDf.withColumn("resolution status in resolved/pending", lower(col("Resolution Status")).isin("resolved", "pending"))
    reportCols = reportCols :+ "anomaly in -1/1"
    val anomalyCheckDf = resolutionStatusCheckDf.withColumn("anomaly in -1/1", col("Anomaly").cast("int").isin(-1, 1))

    anomalyCheckDf
  }

  def isNullCheck(df: DataFrame): DataFrame = {
    df.columns.foldLeft(df)((df, colName) => {
      val is_null_col_name = s"${colName.toLowerCase().replace(' ', '_')}_not_null"
      reportCols = reportCols :+ is_null_col_name
      df.withColumn(is_null_col_name, not(col(colName).isNull))
    })
  }

  def nounLengthCheck(df: DataFrame): DataFrame = {
    val nounCols = Array("First Name", "Last Name", "Address", "City")
    nounCols.foldLeft(df)((df, colName) => {
      val noun_length_col_name = s"${colName.toLowerCase().replace(' ', '_')}_length"
      reportCols = reportCols :+ noun_length_col_name
      df.withColumn(noun_length_col_name, length(col(colName)) > lit(1))
    })
  }

  def dateValidationCheck(df: DataFrame): DataFrame = {
    val dateCols = Array("Date Of Account Opening", "Last Transaction Date", "Transaction Date", "Approval/Rejection Date", "Payment Due Date", "Last Credit Card Payment Date", "Feedback Date", "Resolution Date")
    dateCols.foldLeft(df)((df, colName) => {
      val date_col_name = s"${colName.toLowerCase().replace(' ', '_')}_valid"
      reportCols = reportCols :+ date_col_name
      df.withColumn(date_col_name, to_date(col(colName), "MM/dd/yyyy").isNotNull)
    })
  }

  private def accountBalanceAfterTransactionCheck(transactionAmountCheckDf: DataFrame) = {
    val accountBalance = col("Account Balance").cast("double")
    val transactionAmount = col("Transaction Amount").cast("double")
    val accountBalanceAfterTransaction = col("Account Balance After Transaction").cast("double")

    val transactionSumCheck = round(accountBalance + transactionAmount, 2)
    val transactionDiffCheck = round(accountBalance - transactionAmount, 2)

    val isTransactionValid = accountBalanceAfterTransaction.equalTo(transactionSumCheck)
      .or(accountBalanceAfterTransaction.equalTo(transactionDiffCheck))

    val accountBalanceAfterTransactionCheckDf = transactionAmountCheckDf
      .withColumn("account balance after transaction is valid", isTransactionValid)
    accountBalanceAfterTransactionCheckDf
  }

  def generateReport(df: DataFrame): DataFrame = {
    val aggExprs = reportCols.map(c => sum(when(col(c).cast("boolean"), 1).otherwise(0)).alias(c))
    df.agg(aggExprs.head, aggExprs.tail: _*)
  }
}
