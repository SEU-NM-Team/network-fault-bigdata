package edu.cose.seu

import edu.cose.seu.util.AddressSegmentationUtil
import org.junit.jupiter.api.Test

import scala.util.matching.Regex

class RegexTest {

  val addrUtil = new AddressSegmentationUtil()

  @Test
  def addressSegmentation(): Unit = {
    val list = addrUtil.addressSegmentation("丰顺县汤坑镇邓屋管理区新屋下陈宜伟屋内")
    for (x <- list) {
      println(x)
    }
  }


}
