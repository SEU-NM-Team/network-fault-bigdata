package edu.cose.seu
package util

import config.SparkConfig

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object CSVUtil {

  val schema: StructType = StructType(Array(
    StructField("id", IntegerType, nullable = true),
    StructField("fault_type", StringType, nullable = true),
    StructField("acs_way", StringType, nullable = true),
    StructField("fault_1", StringType, nullable = true),
    StructField("fault_2", StringType, nullable = true),
    StructField("sheng", StringType, nullable = true),
    StructField("shi", StringType, nullable = true),
    StructField("address", StringType, nullable = true),
    StructField("num", IntegerType, nullable = true)
  ))

  def read(path: String): DataFrame = {
    return SparkConfig.spark.read
      .option("header", "true")
      .schema(schema)
      .csv(path)
  }

  def write(df: DataFrame, path: String): Unit = {
    df.coalesce(1).write.option("header", "true").csv(path)
  }
}
