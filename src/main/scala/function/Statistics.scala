package edu.cose.seu
package function


import edu.cose.seu.config.SparkConfig
import edu.cose.seu.config.SparkConfig.{field, spark}
import edu.cose.seu.util.TimeUtil.getTime
import edu.cose.seu.util.{CSVUtil, JDBCUtil}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, Row}

/**
 * 统计四个指标：故障数，重障数，故障地点数，重障地点数
 */
object Statistics {

  val schema: StructType = StructType(Array(
    StructField("fault_id", IntegerType, nullable = true),
    StructField("fault_type", StringType, nullable = true),
    StructField("acs_way", StringType, nullable = true),
    StructField("fault_1", StringType, nullable = true),
    StructField("fault_2", StringType, nullable = true),
    StructField("province", StringType, nullable = true),
    StructField("city", StringType, nullable = true),
    StructField("county", StringType, nullable = true),
    StructField("town", StringType, nullable = true),
    StructField("detail", StringType, nullable = true),
    StructField("num", IntegerType, nullable = true),
    StructField("insertTime", TimestampType, nullable = true)
  ))

  /**
   * 从数据库获取故障信息
   */
  val fault_data: DataFrame = JDBCUtil.getTable("fault_data", field("jdbc.url"))

  def statistics(): DataFrame = {
    /**
     * 计算某一县区，发生某种故障的地点总数
     */
    val faultTownDF = fault_data
      .groupBy("province", "city", "acs_way", "fault")
      .count()
    //      .withColumnRenamed("count", "fault_town_num")

    /**
     * 计算某一县区，某种故障发生重障的地点总数
     */
    val againTownDF = fault_data
      .filter("num > 1")
      .withColumn("num", col("num") - 1)
      .groupBy("province", "city", "acs_way", "fault")
      .count()
    //      .withColumnRenamed("count", "again_town_num")

    /**
     * 合并
     */
    val countDF = faultTownDF.join(againTownDF, Seq("province", "city", "county", "fault_1", "fault_2"), "left")

    //    println("countDF:" + countDF.count())


    /**
     * 故障数=某县区同类型故障的总和
     */
    val faultDF = fault_data
      .groupBy("province", "city", "county", "fault_1", "fault_2")
      .sum("num")
    //      .withColumnRenamed("sum(num)", "fault_num")


    /**
     * 重障：某街道故障数>=2即发生了重障
     * 重障数=统计某县区所有发生重障的街道的重障数量，重障数量=num-1
     */
    val againDF = fault_data
      .filter("num > 1")
      .withColumn("num", col("num") - 1)
      .groupBy("province", "city", "county", "fault_1", "fault_2")
      .sum("num")
      .as("again_num")
    //      .withColumnRenamed("sum(num)", "again_num")


    /**
     * 合并
     */
    val sumDF = faultDF.join(againDF, Seq("province", "city", "county", "fault_1", "fault_2"), "left")

    //    println("sumDF:" + sumDF.count())

    /**
     * 合并
     */
    var totalDF = countDF.join(sumDF, Seq("province", "city", "county", "fault_1", "fault_2"))

    //    println("totalDF:" + totalDF.count())

    /**
     * 缺失值处理
     */
    totalDF = totalDF.na.fill(0)

    val tempSeq = totalDF.collect().toList

    /**
     * 标号
     */
    val statistics_id = JDBCUtil.getTable("statistics").count() + 1
    val rowList = tempSeq.zip(Stream from statistics_id.toInt).map(x => {
      Row(
        x._2,
        x._1.get(0),
        x._1.get(1),
        x._1.get(2),
        x._1.get(3),
        x._1.get(4),
        x._1.get(5),
        x._1.get(6),
        x._1.get(7),
        x._1.get(8),
        getTime
      )
    })

    val statisticsSchema = StructType(Array(
      StructField("statistics_id", IntegerType, nullable = true),
      StructField("province", StringType, nullable = true),
      StructField("city", StringType, nullable = true),
      StructField("county", StringType, nullable = true),
      StructField("fault_1", StringType, nullable = true),
      StructField("fault_2", StringType, nullable = true),
      StructField("fault_address_num", LongType, nullable = true),
      StructField("again_address_num", LongType, nullable = true),
      StructField("fault_num", LongType, nullable = true),
      StructField("again_num", LongType, nullable = true),
      StructField("insert_time", TimestampType, nullable = true)
    ))


    val statisticsDF = spark.createDataFrame(spark.sparkContext.parallelize(rowList), statisticsSchema)

    return statisticsDF
  }


  def main(args: Array[String]): Unit = {
    val myDF = statistics()
    JDBCUtil.writeTable(myDF, "statistics", "append")
  }
}
