package com.example.bankanalysis.preprocessing

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

abstract class BasePreprocessor {

  /**
   * List of relevant columns for the preprocessor
   * @return List of relevant columns
   */
  def relevantColumns(): List[String]

  /**
   * Select a subset of columns from a DataFrame
   * @param df Input DataFrame
   * @param columns List of columns to select
   * @return DataFrame with selected columns
   */
  def selectColumns(df: DataFrame, columns: List[String]): DataFrame = {
    df.select(columns.head, columns.tail: _*)
  }

  /**
   * Cleans the input DataFrame by removing invalid transactions and filling missing values.
   * @param df Input DataFrame
   * @return Cleaned DataFrame
   */
  def cleanData(df: DataFrame): DataFrame

  /**
   * Preprocesses the input DataFrame by ensuring correct data types and adding new columns.
   * @param df Input DataFrame
   * @return Preprocessed DataFrame
   */
  def preprocessData(df: DataFrame): DataFrame

  /**
   * Processes the input DataFrame by selecting necessary columns, cleaning, preprocessing, and transforming the data.
   * @param df Input DataFrame
   * @return Processed DataFrame
   */
  def process(df: DataFrame): DataFrame = {
    val cleanedData = cleanData(df)
    val selectedColumns = selectColumns(cleanedData, relevantColumns())
    preprocessData(selectedColumns)
  }
}