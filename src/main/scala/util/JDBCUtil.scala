package edu.cose.seu
package util

import edu.cose.seu.config.SparkConfig
import org.apache.spark.sql.DataFrame

object JDBCUtil {

  val default_url: String = SparkConfig.field("jdbc.url");

  /**
   * 读取数据库中的表
   *
   * @param table :String 表名
   * @return dataframe of table
   */
  def getTable(table: String, url: String = default_url): DataFrame = {
    return SparkConfig.spark.read.format("jdbc")
      .option("driver", SparkConfig.field("jdbc.driver"))
      .option("url", url)
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
  def writeTable(myDF: DataFrame, table: String, mode: String = "append", url: String = default_url): Unit = {
    myDF.write.format("jdbc")
      .option("driver", SparkConfig.field("jdbc.driver"))
      .option("url", url)
      .option("dbtable", table)
      .option("user", SparkConfig.field("jdbc.username"))
      .option("password", SparkConfig.field("jdbc.password"))
      .option("truncate", value = true)
      .mode(mode)
      .save()
  }

}
