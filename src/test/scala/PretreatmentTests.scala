package edu.cose.seu

import edu.cose.seu.function.Pretreatment
import edu.cose.seu.function.Pretreatment.cleanDF
import edu.cose.seu.util.AddressSegmentationUtil.addressSegmentation
import edu.cose.seu.util.JDBCUtil
import edu.cose.seu.util.TimeUtil.getTime
import org.apache.spark.sql.Row
import org.junit.jupiter.api.Test

import scala.collection.mutable.ListBuffer

class PretreatmentTests {

  @Test
  def dataProcess(): Unit = {
    var index: Long = 1
    var seq = List.empty[Row]
    val schema = JDBCUtil.getTable("fault_data").schema
    cleanDF.limit(1000).foreach(
      elem => {
        val row = Row(index,
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
        index += 1
      })

    println(seq)
  }
}
