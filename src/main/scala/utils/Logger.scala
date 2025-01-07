package utils

import com.typesafe.config.{ConfigFactory, Config}
import java.io._

object Logger {

  private var applicationConf: Config = ConfigFactory.load("application.conf")
  private var logs_path: String = applicationConf.getString("logger.path")
  private var logs_file: String = s"${System.currentTimeMillis()}/error.log"

  def logMessage(message: String): Unit = {
    write(message)
  }

  def logError(exception: Exception, info: Option[String] = None): Unit = {
    val log = s"Error $info :::: ${exception.getMessage}\n${exception.getStackTrace.mkString("\n")}"
    write(log)
  }

  private def write(log: String): Unit = {
    val file = new File(s"$logs_path/$logs_file")
    if (!file.exists()) {
      println("invoked to create folders & files")
      file.getParentFile.mkdirs()
      file.createNewFile()
    }
    println(s"invoked to write $log in $logs_path/$logs_file")
    val fileWriter = new FileWriter(file, true)
    val pw = new PrintWriter(fileWriter)
    pw.write(log)
    pw.close()
  }

}
