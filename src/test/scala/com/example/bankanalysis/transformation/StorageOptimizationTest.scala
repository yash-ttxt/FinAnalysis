package com.example.bankanalysis.transformation

import com.example.bankanalysis.BaseTest
import com.example.bankanalysis.ingestion.DatasetLoader
import com.example.bankanalysis.preprocessing.BankingPreprocessor
import org.apache.spark.sql.SparkSession

class StorageOptimizationTest extends BaseTest {
  override protected var SparkAppName: String = "StorageOptimizationTest"
  override protected var SparkMaster: String = "local[*]"
  override protected var spark: SparkSession = _

  test("toParquetPartitioned") {
    val df = DatasetLoader.loadBankingDataset("src/test/resources/data/raw/Transformations_Testing.csv")(spark)
    val preprocessedDf = BankingPreprocessor.process(df)
    StorageOptimization.toParquetPartitioned(preprocessedDf, "src/test/resources/data/temp/parquet_format", Array("branch_id"))
    val verificationDf = spark.read.options(Map("inferSchema" -> "true")).parquet("src/test/resources/data/temp/parquet_format")
    val cols = preprocessedDf.columns
    assert(preprocessedDf.select(cols.head, cols.tail: _*).except(verificationDf.select(cols.head, cols.tail: _*)).count() == 0)
  }

  before {
    dropTable("banking_analysis")
  }

  test("testBucketing") {
    val df = DatasetLoader.loadBankingDataset("src/test/resources/data/raw/Transformations_Testing.csv")(spark)
    val preprocessedDf = BankingPreprocessor.process(df)
    val startTsWithoutBucketing = System.currentTimeMillis()
    StorageOptimization.top10TransactionVolumeByCustomerId(preprocessedDf).collect()
    val timeTakenWithoutBucketing = System.currentTimeMillis() - startTsWithoutBucketing
    println(s"Time taken for top10TransactionVolumeByCustomerId without bucketing: ${timeTakenWithoutBucketing} ms")
    StorageOptimization.toBucketedTable(preprocessedDf, "banking_analysis", "customer_id", 8)
    val bucketedData = spark.table("banking_analysis")
    val startTsWithBucketing = System.currentTimeMillis()
    StorageOptimization.top10TransactionVolumeByCustomerId(bucketedData).collect()
    val timeTakenWithBucketing = System.currentTimeMillis() - startTsWithBucketing
    println(s"Time taken for top10TransactionVolumeByCustomerId with bucketing: ${timeTakenWithBucketing} ms")
    assert(timeTakenWithoutBucketing > timeTakenWithBucketing)
  }

  after {
    val path = "src/test/resources/data/temp/parquet_format"
    val fs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(path), spark.sparkContext.hadoopConfiguration)
    fs.delete(new org.apache.hadoop.fs.Path(path), true)

    dropTable("banking_analysis")
  }

  private def dropTable(tableName: String): Unit = {
    val tablePath = s"spark-warehouse/$tableName"

    spark.sql(s"DROP TABLE IF EXISTS $tableName")

    val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val path = new org.apache.hadoop.fs.Path(tablePath)
    if (fs.exists(path)) {
      fs.delete(path, true)
    }
  }
}
