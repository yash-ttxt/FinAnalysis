import com.example.bankanalysis.etl.BatchProcessing
import com.example.bankanalysis.etl.streamProcessing.Executor
import io.github.cdimascio.dotenv.Dotenv
import com.example.bankanalysis.ingestion.DatasetLoader
import com.example.bankanalysis.preprocessing.BankingPreprocessor
import com.example.bankanalysis.transformation.SQL
import utils.SparkSessionProvider
import org.apache.spark.sql.SparkSession
import utils.Logger
import com.example.bankanalysis.etl.streamProcessing.etlJobConstants

object main {
  def main(args: Array[String]): Unit = {
    Logger.logMessage(s"Starting the application: ${System.currentTimeMillis()}")
    implicit val dotenv: Dotenv = Dotenv.load()
    implicit val spark: SparkSession = SparkSessionProvider.getSparkSession(dotenv.get("SPARK_APP_NAME"), dotenv.get("SPARK_MASTER"))

    val streamProcessing = Executor
    streamProcessing.main(etlJobConstants.TRANSACTIONS_WITH_HIGH_TRANSACTION_AMOUNT)
    streamProcessing.main(etlJobConstants.CUSOMTERS_WITH_HIGH_ACCOUNT_BALANCE)
    streamProcessing.main(etlJobConstants.WEEKLY_AVERAGE_TRANSACTION_BY_CUSTOMER)
    streamProcessing.main(etlJobConstants.TOP_10_CUSTOMER_BY_TRANSACTION_VOLUME)
    streamProcessing.main(etlJobConstants.MONTHLY_TRANSACTION_VOLUME_BY_BRANCH)

    spark.stop()
    Logger.logMessage(s"Stopping the application: ${System.currentTimeMillis()}")
  }
}
