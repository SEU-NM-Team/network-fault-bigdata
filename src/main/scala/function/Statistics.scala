package edu.cose.seu
package function


import edu.cose.seu.config.SparkConfig
import edu.cose.seu.util.{CSVUtil, JDBCUtil}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Row}


object Statistics {

  /**
   * 从数据库获取故障信息
   */
  val fault_data: DataFrame = JDBCUtil.getTable("fault_data")


  def main(args: Array[String]): Unit = {

  }
}
