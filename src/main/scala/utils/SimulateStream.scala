// This code here will simulate streaming o transactions from data/raw/Comprehensive_Banking_Database.csv as daily transactions

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, current_date, lit, row_number}
import org.apache.spark.sql.streaming.Trigger

object BankingStream {
  /**
   * This method is responsible for simulating the stream of data
   * @param args: Array[String]
   */
  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession.builder()
      .appName("SimulateStream")
      .master("local[*]")
      .getOrCreate()

    val config: Config = ConfigFactory.load("application.conf")

    val rawData = spark.read.option("header", "true").option("inferSchema", "true").csv("data/raw/Comprehensive_Banking_Database.csv")
      .withColumn("Transaction Date", current_date())

    val indexedRawData = rawData.withColumn(
      "row_num",
      row_number().over(Window.orderBy(lit(1)))
    )

    val rateStream = spark.readStream
      .format("rate")
      .option("rowsPerSecond", 5)
      .load()

    val simulatedStream = rateStream
      .withColumn("row_num", (col("value") % indexedRawData.count()).cast("long") + 1)
      .join(indexedRawData, Seq("row_num"), "inner")
      .drop("row_num", "timestamp")

    val stream = simulatedStream.writeStream
      .format("json")
      .queryName("banking_transactions")
      .trigger(Trigger.ProcessingTime("1 seconds"))
      .option("checkpointLocation", config.getString("spark.stream.checkpointPath"))
      .option("path", config.getString("spark.stream.path"))
      .start()

    stream.awaitTermination(4*60*1000)
  }
}