package edu.cose.seu
package function

import util.CSVUtil

import edu.cose.seu.config.SparkConfig.field
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, functions}
import org.apache.spark.sql.functions.{col, lit}

/**
 * 生成风险等级
 */
object Risk {
  val faultDF: DataFrame = CSVUtil.read(field("file.fault_data_output"))

  def generateRiskLevel(): Unit = {
    val level1 = faultDF.filter("num == 1").groupBy("num").count()
      .withColumnRenamed("num", "type")
      .withColumn("type", lit("普通用户"))

    val level2 = faultDF.filter("num == 2").groupBy("num").count()
      .withColumnRenamed("num", "type")
      .withColumn("type", lit("低危用户"))

    val level3 = faultDF.filter("num == 3").groupBy("num").count()
      .withColumnRenamed("num", "type")
      .withColumn("type", lit("中危用户"))

    val level4 = faultDF.filter("num > 3")
      .groupBy("num")
      .count()
      .agg(functions.sum("count"))
      .withColumnRenamed("sum(count)", "count")
      .withColumn("type", lit("高危用户"))

    val resultDF = level1.unionByName(level2)
      .unionByName(level3)
      .unionByName(level4)
      .withColumn("id",
        org.apache.spark.sql.functions.row_number().over(Window.partitionBy().orderBy(col("count"))))

    CSVUtil.write(resultDF, field("file.risk_output"))
    return resultDF
  }

  def main(args: Array[String]): Unit = {
    generateRiskLevel()
  }
}
