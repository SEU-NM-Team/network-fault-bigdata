package edu.cose.seu
package util

import scala.util.matching.Regex

object AddressSegmentationUtil {

  /**
   * 对三级地址做切割
   *
   * @param address :String
   * @return addressList :List[String]
   */
  def addressSegmentation(address: String): List[String] = {
    val pattern: Regex = new Regex("(.{0,4}(县|区|市))?(.*?(乡|镇|街道办|街道|办事处|地区))?(.+)")
    val result = pattern.findAllIn(address)
    return List(result.group(1), result.group(3), result.group(5))
  }


}
