package edu.cose.seu

import edu.cose.seu.config.SparkConfig
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable
import scala.collection.mutable.Set

object Statistics {
  /**
   * 从数据库获取故障信息
   */
  val fault_data: DataFrame = SparkConfig.getTable("fault_data")

  def countCounty(): mutable.Set[String] = {
    val collection: Array[Row] = fault_data.collect();
    val addressSet: mutable.Set[String] = mutable.Set()
    for (x <- collection) {
      addressSet.add(x(6).toString)
    }
    return addressSet
  }


  def main(args: Array[String]): Unit = {
    for (x <- fault_data.groupBy("fault_2").count()) {
      println(x)
    }
  }

}