import com.example.bankanalysis.etl.BatchProcessing
import com.example.bankanalysis.etl.StreamProcessing
import io.github.cdimascio.dotenv.Dotenv
import com.example.bankanalysis.ingestion.DatasetLoader
import com.example.bankanalysis.preprocessing.BankingPreprocessor
import com.example.bankanalysis.transformation.SQL
import utils.SparkSessionProvider
import org.apache.spark.sql.SparkSession
import utils.Logger


object main {
  def main(args: Array[String]): Unit = {
    Logger.logMessage(s"Starting the application: ${System.currentTimeMillis()}")
    implicit val dotenv: Dotenv = Dotenv.load()
    implicit val spark: SparkSession = SparkSessionProvider.getSparkSession(dotenv.get("SPARK_APP_NAME"), dotenv.get("SPARK_MASTER"))


//    val batch = new Batch()
//    batch.main()

    val streamProcessing = new StreamProcessing()
    streamProcessing.main()

    spark.stop()
    Logger.logMessage(s"Stopping the application: ${System.currentTimeMillis()}")
  }
}
