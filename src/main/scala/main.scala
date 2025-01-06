import com.example.bankanalysis.etl.Batch
import io.github.cdimascio.dotenv.Dotenv
import com.example.bankanalysis.ingestion.DatasetLoader
import com.example.bankanalysis.preprocessing.BankingPreprocessor
import com.example.bankanalysis.transformation.SQL
import utils.SparkSessionProvider
import org.apache.spark.sql.SparkSession

import java.time.LocalDate
import java.time.format.DateTimeFormatter

object main {
  def main(args: Array[String]): Unit = {
    val dotenv = Dotenv.load()
    implicit val spark: SparkSession = SparkSessionProvider.getSparkSession(dotenv.get("SPARK_APP_NAME"), dotenv.get("SPARK_MASTER"))

    Batch.main(spark)

    spark.stop()
  }
}
