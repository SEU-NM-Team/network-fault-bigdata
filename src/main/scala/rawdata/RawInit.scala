package edu.cose.seu
package rawdata

import config.SparkConfig.{field, spark}
import util.{CSVUtil, JDBCUtil}
import util.TextUtil.readtxt

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import java.sql.Date

/**
 * 原始txt文件生成csv文件
 */
object RawInit {

  def rawInit(): Unit = {

    /**
     * 读取workdata
     */
    val workdataDF = readtxt(field("file.workdata"), "\r\n")
    var seq = List.empty[Row]

    /**
     * 处理workdata
     */
    workdataDF.take(workdataDF.count().toInt).foreach(
      elem => {
        val container = new Array[String](16)
        //      println(elem)
        val elements = elem.getString(0).split("\\t")
        //      println(elements.length)

        /**
         * 防止element部分字段为空，出现index error
         */
        for (i <- 0 until elements.length) {
          container(i) = elements(i)
        }
        seq = seq :+
          Row(
            container(2).replace("\'", ""),
            container(0),
            container(1),
            container(3),
            container(4),
            Date.valueOf(container(5)),
            container(6).toInt,
            container(7).toInt,
            container(8).toInt,
            container(9),
            container(10),
            container(11),
            container(12),
            container(13),
            container(14),
            container(15)
          )
      })


    val schema = StructType(Array(
      StructField("fault_id", StringType, nullable = true),
      StructField("province", StringType, nullable = true),
      StructField("city", StringType, nullable = true),
      StructField("user_id", StringType, nullable = true),
      StructField("user_number", StringType, nullable = true),
      StructField("fault_time", DateType, nullable = true),
      StructField("value1", IntegerType, nullable = true),
      StructField("value2", IntegerType, nullable = true),
      StructField("value3", IntegerType, nullable = true),
      StructField("mood", StringType, nullable = true),
      StructField("service", StringType, nullable = true),
      StructField("service_detail", StringType, nullable = true),
      StructField("fault", StringType, nullable = true),
      StructField("fault_type", StringType, nullable = true),
      StructField("acs_way", StringType, nullable = true),
      StructField("detail", StringType, nullable = true)
    ))

    /**
     * 生成csv
     */
    var targetDF = spark.createDataFrame(spark.sparkContext.parallelize(seq), schema)

    targetDF = targetDF.dropDuplicates("fault_id");

    CSVUtil.write(targetDF,field("file.workdata_output"))
  }

  def main(args: Array[String]): Unit = {
    rawInit()
  }

}
