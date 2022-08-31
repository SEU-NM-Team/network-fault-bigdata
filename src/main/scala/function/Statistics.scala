package edu.cose.seu
package function


import edu.cose.seu.config.SparkConfig
import edu.cose.seu.config.SparkConfig.{field, spark}
import edu.cose.seu.util.TimeUtil.getTime
import edu.cose.seu.util.{CSVUtil, JDBCUtil}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lit, to_timestamp}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, Row, functions}

/**
 * 统计四个指标：故障数，重障数，故障地点数，重障地点数
 */
object Statistics {

  /**
   * 从数据库获取故障信息
   */
  val fault_data: DataFrame = CSVUtil.read(field("file.fault_data_output"))


  def statistics(): DataFrame = {
    /**
     * 计算某一县区，发生某种故障的地点总数
     */
    val faultAddressDF = fault_data
      .groupBy("province", "city", "acs_way", "fault")
      .count()
      .withColumnRenamed("count", "fault_address_num")

    /**
     * 计算某一县区，某种故障发生重障的地点总数
     */
    val againAddressDF = fault_data
      .filter("num > 1")
      .groupBy("province", "city", "acs_way", "fault")
      .count()
      .withColumnRenamed("count", "again_address_num")

    /**
     * 合并
     */
    val countDF = faultAddressDF.join(againAddressDF, Seq("province", "city", "acs_way", "fault"), "left")

    //    println("countDF:" + countDF.count())


    /**
     * 故障数=某市同类型故障的总和
     */
    val faultDF = fault_data
      .groupBy("province", "city", "acs_way", "fault")
      .agg(functions.sum("num"))
      .withColumnRenamed("sum(num)", "fault_num")


    /**
     * 重障：某街道故障数>=2即发生了重障
     * 重障数=统计某市所有发生重障的街道的重障数量，重障数量=num-1
     */
    val againDF = fault_data
      .filter("num > 1")
      .withColumn("num", col("num") - 1)
      .groupBy("province", "city", "acs_way", "fault")
      .agg(functions.sum("num"))
      .as("again_num")
      .withColumnRenamed("sum(num)", "again_num")


    /**
     * 合并
     */
    val sumDF = faultDF.join(againDF, Seq("province", "city", "acs_way", "fault"), "left")

    //    println("sumDF:" + sumDF.count())

    /**
     * 合并
     */
    var totalDF = countDF.join(sumDF, Seq("province", "city", "acs_way", "fault"))

    //    println("totalDF:" + totalDF.count())

    /**
     * 缺失值处理
     */
    totalDF = totalDF.na.fill(0)


    /**
     * 标号
     */
    val statisticsDF = totalDF
      .withColumn("statistics_id",
        org.apache.spark.sql.functions.row_number().over(Window.partitionBy().orderBy(
          col("province"),
        ))
      )
      .withColumn("insert_time", to_timestamp(lit(getTime)))

    CSVUtil.write(statisticsDF, field("file.statistics_output"))

    return statisticsDF
  }


  def main(args: Array[String]): Unit = {
    statistics()
  }
}
