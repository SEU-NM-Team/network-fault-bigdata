package edu.cose.seu
package function

import util.{CSVUtil, JDBCUtil}

import edu.cose.seu.config.SparkConfig
import edu.cose.seu.config.SparkConfig.spark
import edu.cose.seu.util.AddressSegmentationUtil.addressSegmentation
import edu.cose.seu.util.TimeUtil.getTime
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, TimestampType}

object Pretreatment {

  val sourceDF: DataFrame = CSVUtil.read(SparkConfig.field("file.path"))

  /**
   * 数据清洗
   * <p>1.去除空行</p>
   * <p>2.去除id为空与地址为空的数据</p>
   * <p>3.去除id重复的数据</p>
   * <p>4.缺失值处理</p>
   * <p>5.异常值处理</p>
   *
   * @return cleanDF:DataFrame
   */
  def dataClean(sourceDF: DataFrame): DataFrame = {
    /**
     * 去除空行
     */
    var targetDF = sourceDF.na.drop("all")

    /**
     * 去除id为空与地址为空的数据
     */
    targetDF = targetDF.na.drop(Seq("id", "sheng", "shi", "address"))

    /**
     * 去除id重复的数据
     */
    targetDF = targetDF.dropDuplicates("id");

    /**
     * 缺失值处理
     */
    targetDF = targetDF.na.fill(Map(
      "fault_type" -> "未标注",
      "acs_way" -> "未标注",
      "fault_1" -> "其他错误提示",
      "fault_2" -> "未知",
      "num" -> 1
    ))

    /**
     * 异常值处理
     */
    targetDF = targetDF
      .filter(col("shi").rlike(".+?(市|州|地区)"))
      .filter(col("sheng").rlike(".+?(省|市|自治区)"))
      .filter("length(fault_2)<30")

    return targetDF
  }

  private var index: Int = 1
  private var seq = List.empty[Row]

  /**
   * <p>数据处理</p>
   * <p>1.修整数据格式</p>
   * <p>2.缺失值处理</p>
   *
   * @return
   */
  def dataProcess(cleanDF: DataFrame): DataFrame = {

    val schema = StructType(Array(
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
      StructField("insert_time", TimestampType, nullable = true)
    ))

    cleanDF.limit(1000).foreach(
      elem => {
        val row = Row(index,
          elem(1),
          elem(2),
          elem(3),
          elem(4),
          elem(5),
          elem(6),
          addressSegmentation(elem(7).toString).head,
          addressSegmentation(elem(7).toString)(1),
          addressSegmentation(elem(7).toString)(2),
          elem(8),
          getTime)
        seq = seq :+ row
        index += 1
      })
    var myDF = spark.createDataFrame(spark.sparkContext.parallelize(seq), schema)

    myDF = myDF.na.fill(Map(
      "county" -> "未知县区",
      "town" -> "未知乡镇",
      "detail" -> "未知地址"
    ))

    return myDF
  }

  def pretreatment(): Unit = {
    val cleanDF = dataClean(sourceDF)
    val faultDF = dataProcess(cleanDF)
  }


  def main(args: Array[String]): Unit = {

  }

}