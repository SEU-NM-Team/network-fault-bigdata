package edu.cose.seu
package util

import config.SparkConfig

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object CSVUtil {

  def read(path: String): DataFrame = {
    return SparkConfig.spark.read
      .option("header", "true")
      .csv(path)
  }

  def write(df: DataFrame, path: String): Unit = {
    df.coalesce(1)
      .write
      .option("header", "true")
      .mode("overwrite")
      .csv(path)
  }
}
