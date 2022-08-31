package edu.cose.seu

import function.FaultDataGenerator

import edu.cose.seu.config.SparkConfig
import edu.cose.seu.rawdata.RawPretreatment
import edu.cose.seu.util.{CSV2JDBCUtil, CSVUtil, JDBCUtil}
import SparkConfig.field


object SparkApplicationDataAdder {
  def main(args: Array[String]): Unit = {
    RawPretreatment.rawPretreatment()
    FaultDataGenerator.generateFaultData()
    CSV2JDBCUtil.csv2jdbc(field("file.source_output"), "source_data", "append", field("jdbc.url"))
    CSV2JDBCUtil.csv2jdbc(field("file.fault_data_output"), "fault_data", "append", field("jdbc.url"))
  }
}
