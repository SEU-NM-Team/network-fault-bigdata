package edu.cose.seu
package util

import edu.cose.seu.config.SparkConfig
import org.apache.spark.sql.DataFrame

object JDBCUtil {

  /**
   * 读取数据库中的表
   *
   * @param table :String 表名
   * @return dataframe of table
   */
  def getTable(table: String): DataFrame = {
    return SparkConfig.spark.read.format("jdbc")
      .option("driver", SparkConfig.field("jdbc.driver"))
      .option("url", SparkConfig.field("jdbc.url"))
      .option("dbtable", table)
      .option("user", SparkConfig.field("jdbc.username"))
      .option("password", SparkConfig.field("jdbc.password"))
      .load()
  }

  /**
   * 将DataFrame写入数据库
   *
   * @param myDF  :DataFrame 需要写入的DataFrame
   * @param table :String 表名
   * @param mode  :String 写入模式——"error","append","overwrite",	"ignore"
   *
   */
  def writeTable(myDF: DataFrame, table: String, mode: String = "append"): Unit = {
    myDF.write.format("jdbc")
      .option("driver", SparkConfig.field("jdbc.driver"))
      .option("url", SparkConfig.field("jdbc.url"))
      .option("dbtable", table)
      .option("user", SparkConfig.field("jdbc.username"))
      .option("password", SparkConfig.field("jdbc.password"))
      .mode(mode)
      .save()
  }

}
