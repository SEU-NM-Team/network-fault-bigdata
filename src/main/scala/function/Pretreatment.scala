package edu.cose.seu
package function

import util.{CSVUtil, JDBCUtil}

import edu.cose.seu.config.SparkConfig
import edu.cose.seu.config.SparkConfig.spark
import edu.cose.seu.util.AddressSegmentationUtil.addressSegmentation
import edu.cose.seu.util.CSVUtil.sourceSchema
import edu.cose.seu.util.TimeUtil.getTime
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, TimestampType}

object Pretreatment {

  val sourceDF: DataFrame = CSVUtil.read(sourceSchema, SparkConfig.field("file.path"))

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


  private var seq = List.empty[Row]

  /**
   * <p>数据处理</p>
   * <p>1.修整数据格式</p>
   * <p>2.缺失值处理</p>
   * <p>3.排序</p>
   *
   * @return faultDF:DataFrame 故障数据
   */
  def dataProcess(cleanDF: DataFrame): DataFrame = {

    val seqSchema = StructType(Array(
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

    val faultDataSchema = StructType(Array(
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

    //    /**
    //     * 类型转换,便于遍历
    //     */
    //    val cleanArray = cleanDF.limit(200000).collect()

    println("cleanDF:" + cleanDF.count())

    /**
     * 将cleanDF的地址进行分解，并装入Seq
     */
    cleanDF.take(cleanDF.count().toInt).foreach(
      elem => {
        val addressList = addressSegmentation(elem(7).toString)
        val row = Row(
          elem(1),
          elem(2),
          elem(3),
          elem(4),
          elem(5),
          elem(6),
          addressList.head,
          addressList(1),
          addressList(2),
          elem(8),
          getTime)
        seq = seq :+ row
      })

    println("seq:" + seq.size)

    /**
     * Seq转DF，便于后续操作
     */
    var seqDF = spark.createDataFrame(spark.sparkContext.parallelize(seq), seqSchema)

    /**
     * 缺失值处理
     */
    seqDF = seqDF.na.fill(Map(
      "county" -> "未知县区",
      "town" -> "未知乡镇",
      "detail" -> "未知地址"
    ))

    /**
     * 异常值处理
     */
    seqDF = seqDF
      .filter(col("county").rlike(SparkConfig.field("dict.countyFilter")))
      .filter(col("town").rlike(SparkConfig.field("dict.townFilter")))


    /**
     * Seq转List
     */
    val tempSeq = seqDF.collect().toList

    /**
     * 创建rowList,添加序号
     */
    val fault_id = JDBCUtil.getTable("fault_data").count() + 1
    val rowList = tempSeq.zip(Stream from fault_id.toInt).map(x => {
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
        x._1.get(9),
        x._1.get(10),
      )
    })

    println("rowList:" + rowList.size)

    /**
     * rowList转DF，完成数据处理
     */
    val myDF = spark.createDataFrame(spark.sparkContext.parallelize(rowList), faultDataSchema)

    return myDF
  }


  /**
   * 预处理数据，返回故障数据，并写入数据库
   *
   * @return faultDF:DataFrame——完整故障信息
   */
  def pretreatment(): DataFrame = {
    val cleanDF = dataClean(sourceDF)
    val faultDF = dataProcess(cleanDF)
    JDBCUtil.writeTable(faultDF, "fault_data_copy", "append")
    //    CSVUtil.write(faultDF, "fault_data_1")
    return faultDF
  }


  def main(args: Array[String]): Unit = {
    val faultDF = pretreatment()
    println("faultDF:" + faultDF.count())
    //    val faultDF1 = faultDF.filter(col("county").rlike(SparkConfig.field("dict.filter")))
    //    println(faultDF1.count())
    //    println(sourceDF.count())
  }

}