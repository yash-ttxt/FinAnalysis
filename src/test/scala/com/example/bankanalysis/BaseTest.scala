package com.example.bankanalysis

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import utils.SparkSessionProvider

abstract class BaseTest extends AnyFunSuite with BeforeAndAfterAll with BeforeAndAfter {
  protected var SparkAppName: String
  protected var SparkMaster: String
  protected var spark: SparkSession

  override def beforeAll(): Unit = {
    spark = initializeSparkSession()
  }

  override def afterAll(): Unit = {
    stopSparkSession()
  }

  protected def initializeSparkSession(): SparkSession = {
    SparkSessionProvider.getSparkSession(SparkAppName, SparkMaster)
  }

  protected def stopSparkSession(): Unit = {
    spark.stop()
  }
}
