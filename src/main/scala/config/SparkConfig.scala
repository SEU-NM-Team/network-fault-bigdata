package edu.cose.seu
package config

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkConfig {

  /**
   * 读取配置文件
   */
  val config: Config = ConfigFactory.load("application.conf")

  /**
   * 获取配置文件中的字段
   *
   * @param str :String
   * @return field
   */
  def field(str: String): String = {
    return config.getString(str)
  }

  /**
   * SparkSession对象
   */
  val spark: SparkSession = SparkSession.builder()
    .appName(field("spark.appName"))
    .master(field("spark.master"))
    .getOrCreate()

  /**
   * 读取字符串中的表
   *
   * @param table :String
   * @return dataframe of table
   */

  def getTable(table: String): DataFrame = {
    return spark.read.format("jdbc")
      .option("driver", field("jdbc.driver"))
      .option("url", field("jdbc.url"))
      .option("dbtable", table)
      .option("user", field("jdbc.username"))
      .option("password", field("jdbc.password"))
      .load()
  }

}
