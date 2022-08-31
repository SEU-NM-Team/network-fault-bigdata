package edu.cose.seu

import edu.cose.seu.function.FaultDataGenerator
import edu.cose.seu.function.FaultDataGenerator.dataClean
import edu.cose.seu.util.AddressSegmentationUtil.addressSegmentation
import edu.cose.seu.util.JDBCUtil
import edu.cose.seu.util.TimeUtil.getTime
import org.apache.spark.sql.Row
import org.junit.jupiter.api.Test

import scala.collection.mutable.ListBuffer

class FaultDataGeneratorTests {

  var seq = List.empty[Row]

  @Test
  def dataProcess(): Unit = {
    val cleanDF = dataClean(FaultDataGenerator.sourceDF)
    cleanDF.limit(100).foreach(
      elem => {
        val row = Row(
          elem(1),
          elem(2),
          elem(3),
          elem(4),
          elem(5),
          elem(6),
          addressSegmentation(elem(7).toString).head,
          addressSegmentation(elem(7).toString)(1),
          addressSegmentation(elem(7).toString)(2),
          elem(8),
          getTime)
        seq = seq :+ row
      })

    val myRDD = seq.zip(Stream from 1)

    val tempRDD = myRDD.map(x => {
      Row(x._1.get(0), x._2)
    })

    println(tempRDD)
  }

  @Test
  def stringTest(): Unit = {
    println("深圳罗湖区".length)
    println("深圳罗湖区".substring(2, 5))
  }
}
