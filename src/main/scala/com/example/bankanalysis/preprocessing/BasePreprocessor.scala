package com.example.bankanalysis.preprocessing

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

abstract class BasePreprocessor {

  /**
   * List of relevant columns for the preprocessor
   * @return List of relevant columns
   */
  protected def relevantColumns(): List[String]

  /**
   * Cleans the input DataFrame by removing invalid transactions and filling missing values.
   * @param df Input DataFrame
   * @return Cleaned DataFrame
   */
  protected def cleanData(df: DataFrame): DataFrame

  /**
   * Select a subset of columns from a DataFrame
   * @param df Input DataFrame
   * @param columns List of columns to select
   * @return DataFrame with selected columns
   */
  protected def selectColumns(df: DataFrame, columns: List[String]): DataFrame = {
    df.select(columns.head, columns.tail: _*)
  }

  /**
   * Renames columns in the input DataFrame
   * @param df Input DataFrame
   * @return DataFrame with renamed columns
   */
  protected def renameColumns(df: DataFrame): DataFrame

  /**
   * Preprocesses the input DataFrame by ensuring correct data types and adding new columns.
   * @param df Input DataFrame
   * @return Preprocessed DataFrame
   */
  protected def preprocessData(df: DataFrame): DataFrame

  /**
   * Processes the input DataFrame by selecting necessary columns, cleaning, preprocessing, and transforming the data.
   * @param df Input DataFrame
   * @return Processed DataFrame
   */
  def process(df: DataFrame): DataFrame = {
    val cleanedData = cleanData(df)
    val selectedColumnsDf = selectColumns(cleanedData, relevantColumns())
    val renamedColumnsDf = renameColumns(selectedColumnsDf)
    preprocessData(renamedColumnsDf)
  }
}