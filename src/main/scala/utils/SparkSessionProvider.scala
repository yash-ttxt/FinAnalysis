package utils

import org.apache.spark.sql.SparkSession

object SparkSessionProvider {
  def getSparkSession(appName: String, master: String): SparkSession = {
    SparkSession.builder
      .appName(appName)
      .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
      .master(master)
      .getOrCreate()
  }
}
