package edu.cose.seu
package util

import java.sql.Timestamp
import java.util.Date

object TimeUtil {

  def getTime: Timestamp = {
    val date = new Date()
    return new Timestamp(date.getTime)
  }
}
