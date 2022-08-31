package edu.cose.seu
package util

object CSV2JDBCUtil {

  def csv2jdbc(path: String, table: String, mode: String, url: String): Unit = {
    val tempDF = CSVUtil.read(path)
    JDBCUtil.writeTable(tempDF, table, mode, url)
  }

}
