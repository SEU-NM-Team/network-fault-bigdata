package edu.cose.seu

import SparkConfig.spark

import org.apache.spark.sql.DataFrame
import org.junit.jupiter.api.Test


class JdbcDFTest {

  @Test
  def jdbcDF(): Unit = {
    spark.read.format("jdbc")
      .option("driver", SparkConfig.field("jdbc.driver"))
      .option("url", SparkConfig.field("jdbc.url"))
      .option("dbtable", "fault_data")
      .option("user", SparkConfig.field("jdbc.username"))
      .option("password", SparkConfig.field("jdbc.password"))
      .load()
      .show()
  }

  @Test
  def password(): Unit = {
    println(SparkConfig.field("jdbc.password"))
  }

}
