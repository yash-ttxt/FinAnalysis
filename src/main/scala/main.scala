package main

import org.apache.spark.sql.SparkSession
import io.github.cdimascio.dotenv.Dotenv
import com.example.bankanalysis.ingestion.DatasetLoader
import com.example.bankanalysis.preprocessing.BankingPreprocessor
import utils.SparkSessionProvider
import java.time.LocalDate
import java.time.format.DateTimeFormatter

object BankingAnalysisRunner {
  def main(args: Array[String]): Unit = {
    val dotenv = Dotenv.load()
    val spark = SparkSessionProvider.getSparkSession(dotenv.get("SPARK_APP_NAME"), dotenv.get("SPARK_MASTER"))

    // Add the ETL code here.
    val currentDate = LocalDate.now().format(DateTimeFormatter.ofPattern("dd-MM-yyyy"))
    val dataset = DatasetLoader.loadBankingDataset(spark, dotenv.get("RAW_BANKING_DATASET_PATH")+s"_${currentDate}.csv")
    val preprocessor = BankingPreprocessor
    val processedData = preprocessor.process(dataset)
//    processedData.write.mode("append").parquet(dotenv.get("PROCESSED_PARQUET_DATA_PATH"))
//    processedData.write.mode("append").json(dotenv.get("PROCESSED_JSON_DATA_PATH"))

    spark.stop()
  }
}
