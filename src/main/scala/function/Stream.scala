package edu.cose.seu
package function

import util.CSVUtil

import edu.cose.seu.config.SparkConfig.field
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, date_format}

/**
 * 流式统计数据
 */
object Stream {

  val sourceDF: DataFrame = CSVUtil.read(field("file.source_output"))

  def streamCount(): DataFrame = {
    val targetDF = sourceDF
      .filter(date_format(col("fault_time"), "yyyy") > 2015)
      .withColumn("fault_time", date_format(col("fault_time"), "yyyy-MM"))
      .groupBy("province", "fault_time")
      .count()
      .withColumn("id",
        org.apache.spark.sql.functions.row_number().over(Window.partitionBy().orderBy(col("province"), col("fault_time"))))

    CSVUtil.write(targetDF, field("file.stream_output"))
    return targetDF
  }

  def main(args: Array[String]): Unit = {
    streamCount()
  }

}
