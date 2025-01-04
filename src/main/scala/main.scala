import org.apache.spark.sql.SparkSession
import io.github.cdimascio.dotenv.Dotenv

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master(Dotenv.load().get("SPARK_MASTER"))
      .appName(Dotenv.load().get("SPARK_APP_NAME"))
      .getOrCreate()

    // Add your code here

    spark.stop()
  }
}