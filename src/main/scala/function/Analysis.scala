package edu.cose.seu
package function

import util.{CSVUtil, JDBCUtil, JSONUtil}

import edu.cose.seu.config.SparkConfig
import edu.cose.seu.config.SparkConfig.field
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lit, sum}

object Analysis {

  val statisticsDF: DataFrame = CSVUtil.read(
    JDBCUtil.getTable("statistics").schema,
    field("file.statistics_output")
  )

  val faultDF: DataFrame = CSVUtil.read(
    JDBCUtil.getTable("fault_data").schema,
    field("file.fault_data_output")
  )

  /**
   * 分析省市统计
   */
  def generateCitySum(): Unit = {
    val cityDF = statisticsDF.groupBy("province", "city")
      .sum("fault_num")
      .withColumnRenamed("sum(fault_num)", "fault_sum")
      .withColumn("id",
        org.apache.spark.sql.functions.row_number().over(Window.partitionBy().orderBy(col("province"), -col("fault_sum"))))
    cityDF.show(100)
    JDBCUtil.writeTable(cityDF, "city_sum", "overwrite", SparkConfig.field("jdbc.plot_url"))
  }

  /**
   * 分析省市故障地点
   */
  def generateRegionSum(): Unit = {
    val cityDF = statisticsDF.groupBy("province", "city")
      .sum("fault_address_num")
      .withColumnRenamed("sum(fault_address_num)", "address_sum")
      .withColumn("id",
        org.apache.spark.sql.functions.row_number().over(Window.partitionBy().orderBy(col("province"), -col("address_sum"))))
    cityDF.show(100)
    JDBCUtil.writeTable(cityDF, "address_sum", "overwrite", SparkConfig.field("jdbc.plot_url"))
  }

  /**
   * 分析各省一类故障
   */
  def generateFault1(): Unit = {
    val faultDF1 = faultDF
      .groupBy("province", "fault_1")
      .sum("num")
      .withColumnRenamed("sum(num)", "sum")
      .withColumn("id",
        org.apache.spark.sql.functions.row_number().over(Window.partitionBy().orderBy(col("province"), -col("sum"))))
    faultDF1.show(1000)
    JDBCUtil.writeTable(faultDF1, "fault_1", "overwrite", SparkConfig.field("jdbc.plot_url"))
  }

  /**
   * 分析省市二类故障
   */
  def generateFault2(): Unit = {
    val faultDF1 = faultDF
      .groupBy("province", "fault_2")
      .sum("num")
      .withColumnRenamed("sum(num)", "sum")
      .withColumn("id",
        org.apache.spark.sql.functions.row_number().over(Window.partitionBy().orderBy(col("province"), -col("sum"))))
    faultDF1.show(1000)
    JDBCUtil.writeTable(faultDF1, "fault_2", "overwrite", SparkConfig.field("jdbc.plot_url"))
  }

  /**
   * 分析四种接入方式——宽带业务，IPTV,增值业务，未标注
   */
  def generateAcsWay(): Unit = {
    val acswayDF = faultDF
      .groupBy("acs_way")
      .sum("num")
      .withColumnRenamed("sum(num)", "sum")
      .orderBy(-col("sum"))
    // acswayDF.show(1000)
    val DF1 = acswayDF
      .filter(col("acs_way").rlike("宽带"))
      .agg(sum("sum"))
      .withColumnRenamed("sum(sum)", "sum")
      .withColumn("acs_way", lit("宽带业务"))
    DF1.show()
    val DF2 = acswayDF
      .filter(col("acs_way").rlike("IPTV"))
      .agg(sum("sum"))
      .withColumnRenamed("sum(sum)", "sum")
      .withColumn("acs_way", lit("IPTV"))
    val DF3 = acswayDF
      .filter(col("acs_way").rlike("增值"))
      .agg(sum("sum"))
      .withColumnRenamed("sum(sum)", "sum")
      .withColumn("acs_way", lit("增值业务"))
    val DF4 = acswayDF.filter("acs_way=='未标注'")
      .agg(sum("sum"))
      .withColumnRenamed("sum(sum)", "sum")
      .withColumn("acs_way", lit("未标注"))
    val targetDF = DF1
      .unionByName(DF2)
      .unionByName(DF3)
      .unionByName(DF4)
      .withColumn("id",
        org.apache.spark.sql.functions.row_number().over(Window.partitionBy().orderBy(-col("sum"))))
    JDBCUtil.writeTable(targetDF, "acs_way_sum", "overwrite", SparkConfig.field("jdbc.plot_url"))
  }

  /**
   * 统计省份对比
   */
  def generateProvinceContrast(): Unit = {
    val DF1 = statisticsDF
      .groupBy("province")
      .sum("fault_address_num")
      .withColumnRenamed("sum(fault_address_num)", "sum")
      .withColumn("id",
        org.apache.spark.sql.functions.row_number().over(Window.partitionBy().orderBy(-col("sum"))))
    JDBCUtil.writeTable(DF1, "fault_address_contrast", "overwrite", SparkConfig.field("jdbc.plot_url"))

    val DF2 = statisticsDF
      .groupBy("province")
      .sum("again_address_num")
      .withColumnRenamed("sum(again_address_num)", "sum")
      .withColumn("id",
        org.apache.spark.sql.functions.row_number().over(Window.partitionBy().orderBy(-col("sum"))))
    JDBCUtil.writeTable(DF2, "again_address_contrast", "overwrite", SparkConfig.field("jdbc.plot_url"))

    val DF3 = statisticsDF
      .groupBy("province")
      .sum("fault_num")
      .withColumnRenamed("sum(fault_num)", "sum")
      .withColumn("id",
        org.apache.spark.sql.functions.row_number().over(Window.partitionBy().orderBy(-col("sum"))))
    JDBCUtil.writeTable(DF3, "fault_num_contrast", "overwrite", SparkConfig.field("jdbc.plot_url"))

    val DF4 = statisticsDF
      .groupBy("province")
      .sum("again_num")
      .withColumnRenamed("sum(again_num)", "sum")
      .withColumn("id",
        org.apache.spark.sql.functions.row_number().over(Window.partitionBy().orderBy(-col("sum"))))
    JDBCUtil.writeTable(DF4, "again_num_contrast", "overwrite", SparkConfig.field("jdbc.plot_url"))
  }


  def main(args: Array[String]): Unit = {
    //    generateCitySum()
    //    generateRegionSum()
    //    generateFault1()
    //    generateFault2()
    //    generateAcsWay()
    //    generateProvinceContrast()
    val statistics = CSVUtil.read(JDBCUtil.getTable("statistics").schema, field("file.statistics_output"))
    statistics.select("acs_way").distinct().show()
  }


}
