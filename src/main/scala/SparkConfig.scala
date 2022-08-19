package edu.cose.seu

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}


object SparkConfig {

  val config: Config = ConfigFactory.load("application.conf")

  def field(str: String): String = {
    return config.getString(str)
  }

  val spark: SparkSession = SparkSession.builder()
    .appName(field("spark.appName"))
    .master(field("spark.master"))
    .getOrCreate()

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
