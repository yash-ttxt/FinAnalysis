import com.example.bankanalysis.etl.BatchProcessing
import com.example.bankanalysis.etl.streamProcessing.Executor
import io.github.cdimascio.dotenv.Dotenv
import com.example.bankanalysis.etl.BatchProcessing
import utils.{SparkSessionProvider, Logger, ETLMonitor}
import org.apache.spark.sql.SparkSession
import com.example.bankanalysis.etl.streamProcessing.{etlJobConstants, etlProcessConstants}

/**
 * Main entry point of the application
 */
object main {
  def main(args: Array[String]): Unit = {
    Logger.logMessage(s"Starting the application: ${System.currentTimeMillis()}")
    val dotenv: Dotenv = Dotenv.load()
    implicit val spark: SparkSession = SparkSessionProvider.getSparkSession(sys.env("SPARK_APP_NAME"), sys.env("SPARK_MASTER"))

    // Expects either BATCH or STREAM as the first argument
    val option = args(0)
    option match {
      case etlProcessConstants.BATCH =>
        Logger.logMessage("Batch processing")
        val batchProcessing = BatchProcessing
        batchProcessing.main()
      case etlProcessConstants.STREAM =>
        Logger.logMessage("Stream processing")
        // For each streaming job, a new class should be created in the streamProcessing package
        val streamJob = args(1)
        val streamProcessing = Executor
        streamProcessing.main(streamJob)
      case _ => Logger.logError(new Exception("Invalid option"))
    }

    ETLMonitor.writeMonitorLog()
    spark.stop()
    Logger.logMessage(s"Stopping the application: ${System.currentTimeMillis()}")
  }
}
