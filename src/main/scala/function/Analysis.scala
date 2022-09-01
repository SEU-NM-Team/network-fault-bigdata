package edu.cose.seu
package function

import util.{CSVUtil, JDBCUtil, JSONUtil}

import edu.cose.seu.config.SparkConfig
import edu.cose.seu.config.SparkConfig.field
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lit, sum}

object Analysis {

  val statisticsDF: DataFrame = CSVUtil.read(field("file.statistics_output"))

  val faultDF: DataFrame = CSVUtil.read(field("file.fault_data_output"))

  /**
   * 分析省市统计
   */
  def generateCitySum(): Unit = {
    val cityDF = statisticsDF.groupBy("province", "city")
      .agg(sum("fault_num"))
      .withColumnRenamed("sum(fault_num)", "fault_sum")
      .withColumn("id",
        org.apache.spark.sql.functions.row_number().over(Window.partitionBy().orderBy(col("province"), -col("fault_sum"))))
    //    cityDF.show(100)
    CSVUtil.write(cityDF, field("file.city_sum_output"))
    //    JDBCUtil.writeTable(cityDF, "city_sum", "overwrite", SparkConfig.field("jdbc.plot_url"))
  }

  /**
   * 分析省市故障地点
   */
  def generateAddressSum(): Unit = {
    val cityDF = statisticsDF.groupBy("province", "city")
      .agg(sum("fault_address_num"))
      .withColumnRenamed("sum(fault_address_num)", "address_sum")
      .withColumn("id",
        org.apache.spark.sql.functions.row_number().over(Window.partitionBy().orderBy(col("province"), -col("address_sum"))))
    //    cityDF.show(100)
    CSVUtil.write(cityDF, field("file.address_sum_output"))
    //    JDBCUtil.writeTable(cityDF, "address_sum", "overwrite", SparkConfig.field("jdbc.plot_url"))
  }

  /**
   * 分析各省故障
   */
  def generateFault(): Unit = {
    val faultDF1 = faultDF
      .groupBy("fault")
      .agg(sum("num"))
      .withColumnRenamed("sum(num)", "sum")
      .withColumn("id",
        org.apache.spark.sql.functions.row_number().over(Window.partitionBy().orderBy(-col("sum"))))
      .limit(5)
    //    faultDF1.show(1000)
    CSVUtil.write(faultDF1, field("file.fault_output"))
    //    JDBCUtil.writeTable(faultDF1, "fault", "overwrite", SparkConfig.field("jdbc.plot_url"))
  }

  /**
   * 分析故障类型
   */
  def generateFaultType(): Unit = {
    val faultDF1 = faultDF.groupBy("fault_type")
      .agg(sum("num"))
      .withColumnRenamed("sum(num)", "sum")
      .withColumn("id",
        org.apache.spark.sql.functions.row_number().over(Window.partitionBy().orderBy(-col("sum"))))
    //    faultDF1.show(1000)
    CSVUtil.write(faultDF1, field("file.fault_type_output"))
    //    JDBCUtil.writeTable(faultDF1, "fault", "overwrite", SparkConfig.field("jdbc.plot_url"))
  }

  /**
   * 分析接入方式
   */
  def generateAcsWay(): Unit = {
    val targetDF = statisticsDF
      .groupBy("acs_way")
      .count()
      .withColumnRenamed("count", "sum")
      .withColumn("id",
        org.apache.spark.sql.functions.row_number().over(Window.partitionBy().orderBy(-col("sum"))))
      .limit(4)
    CSVUtil.write(targetDF, field("file.acs_way_output"))
    //    JDBCUtil.writeTable(targetDF, "acs_way_sum", "overwrite", SparkConfig.field("jdbc.plot_url"))
  }

  /**
   * 统计省份对比
   */
  def generateProvinceContrast(): Unit = {
    val DF1 = statisticsDF
      .groupBy("province")
      .agg(sum("fault_address_num"))
      .withColumnRenamed("sum(fault_address_num)", "sum")
      .withColumn("id",
        org.apache.spark.sql.functions.row_number().over(Window.partitionBy().orderBy(-col("sum"))))
    CSVUtil.write(DF1, field("file.fault_address_contrast_output"))
    //    JDBCUtil.writeTable(DF1, "fault_address_contrast", "overwrite", SparkConfig.field("jdbc.plot_url"))

    val DF2 = statisticsDF
      .groupBy("province")
      .agg(sum("again_address_num"))
      .withColumnRenamed("sum(again_address_num)", "sum")
      .withColumn("id",
        org.apache.spark.sql.functions.row_number().over(Window.partitionBy().orderBy(-col("sum"))))
    CSVUtil.write(DF2, field("file.again_address_contrast_output"))
    //    JDBCUtil.writeTable(DF2, "again_address_contrast", "overwrite", SparkConfig.field("jdbc.plot_url"))

    val DF3 = statisticsDF
      .groupBy("province")
      .agg(sum("fault_num"))
      .withColumnRenamed("sum(fault_num)", "sum")
      .withColumn("id",
        org.apache.spark.sql.functions.row_number().over(Window.partitionBy().orderBy(-col("sum"))))
    CSVUtil.write(DF3, field("file.fault_num_contrast_output"))
    //    JDBCUtil.writeTable(DF3, "fault_num_contrast", "overwrite", SparkConfig.field("jdbc.plot_url"))

    val DF4 = statisticsDF
      .groupBy("province")
      .agg(sum("again_num"))
      .withColumnRenamed("sum(again_num)", "sum")
      .withColumn("id",
        org.apache.spark.sql.functions.row_number().over(Window.partitionBy().orderBy(-col("sum"))))
    CSVUtil.write(DF4, field("file.again_num_contrast_output"))
    //    JDBCUtil.writeTable(DF4, "again_num_contrast", "overwrite", SparkConfig.field("jdbc.plot_url"))
  }


  def main(args: Array[String]): Unit = {
    generateCitySum()
    generateAddressSum()
    generateFault()
    generateAcsWay()
    generateProvinceContrast()
  }


}
