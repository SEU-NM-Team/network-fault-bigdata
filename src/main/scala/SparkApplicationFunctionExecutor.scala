package edu.cose.seu

import function.{Analysis, Portrait, Risk, Statistics}

import edu.cose.seu.config.SparkConfig.field
import edu.cose.seu.util.CSV2JDBCUtil
import edu.cose.seu.function.Stream

object SparkApplicationFunctionExecutor {

  def main(args: Array[String]): Unit = {
    Analysis.generateCitySum()
    Analysis.generateAddressSum()
    Analysis.generateFault()
    Analysis.generateFaultType()
    Analysis.generateAcsWay()
    Analysis.generateProvinceContrast()
    Portrait.generatePortrait()
    Risk.generateRiskLevel()
    Stream.streamCount()

    /**
     * 写一系列绘图表
     */
    CSV2JDBCUtil.csv2jdbc(
      field("file.city_sum_output"),
      "city_sum",
      "overwrite",
      field("jdbc.plot_url")
    )

    CSV2JDBCUtil.csv2jdbc(
      field("file.address_sum_output"),
      "address_sum",
      "overwrite",
      field("jdbc.plot_url")
    )

    CSV2JDBCUtil.csv2jdbc(
      field("file.fault_output"),
      "fault",
      "overwrite",
      field("jdbc.plot_url")
    )

    CSV2JDBCUtil.csv2jdbc(
      field("file.fault_type_output"),
      "fault_type",
      "overwrite",
      field("jdbc.plot_url")
    )

    CSV2JDBCUtil.csv2jdbc(
      field("file.acs_way_output"),
      "acs_way",
      "overwrite",
      field("jdbc.plot_url")
    )

    CSV2JDBCUtil.csv2jdbc(
      field("file.fault_address_contrast_output"),
      "fault_address_contrast",
      "overwrite",
      field("jdbc.plot_url")
    )

    CSV2JDBCUtil.csv2jdbc(
      field("file.again_address_contrast_output"),
      "again_address_contrast",
      "overwrite",
      field("jdbc.plot_url")
    )

    CSV2JDBCUtil.csv2jdbc(
      field("file.fault_num_contrast_output"),
      "fault_num_contrast",
      "overwrite",
      field("jdbc.plot_url")
    )

    CSV2JDBCUtil.csv2jdbc(
      field("file.again_num_contrast_output"),
      "again_num_contrast",
      "overwrite",
      field("jdbc.plot_url")
    )

    CSV2JDBCUtil.csv2jdbc(
      field("file.portrait_output"),
      "portrait",
      "overwrite",
      field("jdbc.plot_url")
    )

    CSV2JDBCUtil.csv2jdbc(
      field("file.risk_output"),
      "risk",
      "overwrite",
      field("jdbc.plot_url")
    )

    CSV2JDBCUtil.csv2jdbc(
      field("file.stream_output"),
      "stream",
      "overwrite",
      field("jdbc.plot_url")
    )
  }
}
