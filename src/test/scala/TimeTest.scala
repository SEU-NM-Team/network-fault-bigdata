package edu.cose.seu

import org.junit.jupiter.api.Test

import java.util.Date

import java.sql.Timestamp

class TimeTest {

  @Test
  def getTime(): Unit = {
    val date = new Date()
    val timeStamp = new Timestamp(date.getTime)
    println(timeStamp)
  }
}
