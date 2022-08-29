package edu.cose.seu
package function

import util.{JDBCUtil, JSONUtil}

import edu.cose.seu.config.SparkConfig
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.col

object Analysis {

  val statisticsDF: DataFrame = JDBCUtil.getTable("statistics")

  def generateCitySum(): Unit = {
    val cityDF = statisticsDF.groupBy("province", "city")
      .sum("fault_num")
      .withColumnRenamed("sum(fault_num)", "sum")
      .withColumn("id",
        org.apache.spark.sql.functions.row_number().over(Window.partitionBy().orderBy(col("province"), -col("sum"))))
    cityDF.show(100)
    JDBCUtil.writeTable(cityDF, "city_sum", "append", SparkConfig.field("jdbc.plot_url"))
  }

  def main(args: Array[String]): Unit = {
    generateCitySum()
  }


}
