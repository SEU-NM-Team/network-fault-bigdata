package edu.cose.seu
package util

import config.SparkConfig.spark

import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import spark.implicits._

import java.text.SimpleDateFormat

object TextUtil {

  def readtxt(path: String, lineSep: String): DataFrame = {
    return spark.read
      .option("lineSep", lineSep)
      .text(path)
  }

  def writetxt(df: DataFrame, path: String): Unit = {
    df.write.text(path)
  }

}
