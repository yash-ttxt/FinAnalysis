package main

import org.apache.spark.sql.SparkSession
import io.github.cdimascio.dotenv.Dotenv

object Main {
  def main(args: Array[String]): Unit = {
    val dotenv = Dotenv.load()
    val spark = SparkSession.builder
      .appName(Dotenv.load().get("SPARK_APP_NAME"))
      .master(Dotenv.load().get("SPARK_MASTER"))
      .getOrCreate()

    // Add the ETL code here.

    spark.stop()
  }
}
