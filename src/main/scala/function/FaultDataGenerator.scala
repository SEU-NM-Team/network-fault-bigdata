package edu.cose.seu
package function

import util.{CSVUtil, JDBCUtil}

import edu.cose.seu.config.SparkConfig
import edu.cose.seu.config.SparkConfig.{field, spark}
import edu.cose.seu.util.AddressSegmentationUtil.addressSegmentation
import edu.cose.seu.util.CSVUtil.sourceSchema
import edu.cose.seu.util.TimeUtil.getTime
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{col, lit, to_date, to_timestamp}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, TimestampType}

/**
 * 对source_data进行预处理
 */
object FaultDataGenerator {

  /**
   * 读取source_data
   */
  val sourceDF: DataFrame = CSVUtil.read(
    JDBCUtil.getTable("source_data", field("jdbc.url")).schema,
    field("file.source_output")
  )

  def generateFaultData(): DataFrame = {
    var targetDF = sourceDF.groupBy("province", "city", "detail", "user_id", "acs_way", "fault_type", "fault")
      .count()
      .withColumnRenamed("count", "num")
      .withColumn("fault_id",
        org.apache.spark.sql.functions.row_number().over(Window.partitionBy().orderBy(
          col("province"),
          col("city"),
          -col("num"),
        ))
      )
      .withColumn("insert_time",to_timestamp(lit(getTime)))
    CSVUtil.write(targetDF, field("file.fault_data_output"))
    return targetDF
  }


  def main(args: Array[String]): Unit = {
    generateFaultData()
  }


}