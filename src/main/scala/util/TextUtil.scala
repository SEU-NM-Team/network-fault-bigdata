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
      .csv(path)
  }

  def writetxt(df: DataFrame, path: String): Unit = {
    df.write.text(path)
  }

  def main(args: Array[String]): Unit = {
    val df1 = readtxt("F:/workdata.txt", "\n")
    var seq = List.empty[Row]

    df1.take(df1.count().toInt).foreach(elem => {
      //      println(elem)
      val elements = elem.getString(0).split("\\t")
      //      println(elements.length)
      if (elements.size == 16) {
        seq = seq :+
          Row(
            elements(2).replace("\'", ""),
            elements(0),
            elements(1),
            elements(3),
            elements(4),
            elements(5),
            elements(6).toInt,
            elements(7).toInt,
            elements(8).toInt,
            elements(9),
            elements(10),
            elements(11),
            elements(12),
            elements(13),
            elements(14),
            elements(15)
          )
      }
    })
    val schema = StructType(Array(
      StructField("id", StringType, nullable = true),
      StructField("province", StringType, nullable = true),
      StructField("city", StringType, nullable = true),
      StructField("user_type", StringType, nullable = true),
      StructField("user_number", StringType, nullable = true),
      StructField("fault_time", StringType, nullable = true),
      StructField("col7", IntegerType, nullable = true),
      StructField("col8", IntegerType, nullable = true),
      StructField("col9", IntegerType, nullable = true),
      StructField("mood", StringType, nullable = true),
      StructField("service", StringType, nullable = true),
      StructField("service_detail", StringType, nullable = true),
      StructField("fault_1", StringType, nullable = true),
      StructField("fault_type", StringType, nullable = true),
      StructField("fault_2", StringType, nullable = true),
      StructField("detail", StringType, nullable = true)
    ))
    val df2 = spark.createDataFrame(spark.sparkContext.parallelize(seq), schema)
    CSVUtil.write(df2, "src_data")
  }

}
