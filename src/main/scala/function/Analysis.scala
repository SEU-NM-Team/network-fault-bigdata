package edu.cose.seu
package function

import util.JDBCUtil

import org.apache.spark.sql.DataFrame

object Analysis {

  val statisticsDF:DataFrame = JDBCUtil.getTable("statistics")

  



}
