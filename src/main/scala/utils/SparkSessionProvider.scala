package utils

import org.apache.spark.sql.SparkSession

object SparkSessionProvider {
  /**
   * This method is responsible for getting the SparkSession
   * @param appName: String
   * @param master: String
   * @return SparkSession
   */
  def getSparkSession(appName: String, master: String): SparkSession = {
    SparkSession.builder
      .appName(appName)
      .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
      .master(master)
      .getOrCreate()
  }
}
