package edu.cose.seu

import org.apache.spark.sql.DataFrame

object Statistics {
  val fault_data: DataFrame = SparkConfig.getTable("fault_data")

  def main(args: Array[String]): Unit = {
    fault_data.show()
  }

}
