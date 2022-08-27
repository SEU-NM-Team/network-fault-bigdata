package edu.cose.seu
package function

import util.JDBCUtil

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

object Analysis {

  val statisticsDF: DataFrame = JDBCUtil.getTable("statistics")

  def main(args: Array[String]): Unit = {
    statisticsDF.groupBy("province").sum("fault_num").orderBy(-col("sum(fault_num)")).show(10000)

  }


}
