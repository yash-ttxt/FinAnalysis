package utils

import com.typesafe.config.{Config, ConfigFactory}

import java.io.File

object ETLMonitor {
  private val jobInfo: collection.mutable.Map[String, Map[String, String]] = collection.mutable.Map.empty

  /**
   * This method is responsible for updating the start time of the job
   *
   * @param jobName : String
   */
  def updateJobStartTime(jobName: String): Unit = {
    jobInfo.update(jobName, Map("start" -> System.currentTimeMillis().toString))
  }

  /**
   * This method is responsible for updating the end time of the job
   *
   * @param jobName : String
   */
  def updateJobEndTime(jobName: String): Unit = {
    val jobStartTime = jobInfo(jobName)("start").toLong
    val jobEndTime = System.currentTimeMillis()
    val latency = jobEndTime - jobStartTime

    // Extract existing job information to improve clarity and avoid repetitive lookups
    val existingInfo = jobInfo(jobName)

    // Constants to represent keys for updates
    val EndTimeKey = "end"
    val LatencyKey = "latency"

    // Build the updated job information
    val updatedInfo = existingInfo + (
      EndTimeKey -> jobEndTime.toString,
      LatencyKey -> latency.toString
    )

    // Update the job information map
    jobInfo.update(jobName, updatedInfo)
  }

  /**
   * This method is responsible for updating the status of the job
   *
   * @param jobName : String
   * @param status  : String
   */
  def updateJobStatus(jobName: String, status: String): Unit = {
    if (status == "FAILED" || status == "COMPLETED") {
      updateJobEndTime(jobName)
    }
    jobInfo.update(jobName, jobInfo.getOrElse(jobName, Map.empty) + ("status" -> status))
  }

  /**
   * This method is responsible for writing the monitor log
   */
  def writeMonitorLog(): Unit = {
    val applicationConfig: Config = ConfigFactory.load("application.conf").getConfig("monitor")
    val monitorLogPath: String = applicationConfig.getString("path")
    val filePath = s"$monitorLogPath/etl_monitor_${System.currentTimeMillis()}.log"
    val file = new File(filePath)
    if (!file.exists()) {
      file.getParentFile.mkdirs()
      file.createNewFile()
    }
    val fileWriter = new java.io.FileWriter(file, true)
    val pw = new java.io.PrintWriter(fileWriter)
    pw.write("Job Name,Start Time,End Time,Latency,Status\n")
    jobInfo.foreach { case (jobName, jobDetails) =>
      val startTime = jobDetails.getOrElse("start", "NA")
      val endTime = jobDetails.getOrElse("end", "NA")
      val latency = jobDetails.getOrElse("latency", "NA")
      val status = jobDetails.getOrElse("status", "NA")
      pw.write(s"$jobName,$startTime,$endTime,$latency,$status\n")
    }
    pw.close()
  }

}
