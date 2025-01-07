package com.example.bankanalysis.etl.streamProcessing

import io.github.cdimascio.dotenv.Dotenv
import com.example.bankanalysis.transformation.WindowFunctions
import org.apache.spark.sql.{DataFrame, SparkSession}

class WeeklyAverageTransactionByCustomer extends StreamBase {
  override protected def transform(df: DataFrame)(implicit spark: SparkSession, dotenv: Dotenv): DataFrame = {
    WindowFunctions.weeklyAverageTransactionAmountByCustomer(df)
  }

  override protected def writeStream(df: DataFrame)(implicit spark: SparkSession, dotenv: Dotenv): Unit = {
    df.writeStream
      .outputMode("append")
      .format("parquet")
      .option("path", dotenv.get("weeklyAverageTransactionByCustomer.parquet.outputPath"))
      .option("checkpointLocation", dotenv.get("weeklyAverageTransactionByCustomer.parquet.checkpointLocation"))
      .start()
      .awaitTermination(4 * 60 * 1000) // Todo: Read from config
  }
}
