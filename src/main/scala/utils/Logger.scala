package utils

import com.typesafe.config.{ConfigFactory, Config}
import java.io._

object Logger {

  private var applicationConf: Config = ConfigFactory.load("application.conf")
  private var logs_path: String = applicationConf.getString("logs.path")
  private var logs_file: String = applicationConf.getString(s"${System.currentTimeMillis()}/error.log")

  def logMessage(message: String): Unit = {
    write(message)
  }

  def logError(exception: Exception, info: Option[String] = None): Unit = {
    val log = s"Error $info :::: ${exception.getMessage}\n${exception.getStackTrace.mkString("\n")}"
    write(log)
  }

  private def write(log: String): Unit = {
    val pw = new PrintWriter(new File(s"$logs_path/$logs_file"))
    pw.write(log)
    pw.close()
  }

}
