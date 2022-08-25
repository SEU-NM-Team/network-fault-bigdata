package edu.cose.seu
package util

import edu.cose.seu.config.SparkConfig

import scala.util.matching.Regex

object AddressSegmentationUtil {

  /**
   * 对三级地址做切割
   *
   * @param address :String
   * @return addressList :List[String]
   */
  def addressSegmentation(address: String): List[String] = {
    val pattern: Regex = new Regex(SparkConfig.field("dict.address"))
    val result = pattern.findAllIn(address)

    return List(
      if (result.group(2) == null)
        null
      else if (result.group(2).length < 5)
        result.group(2)
      else
        result.group(2).substring(result.group(2).length - 3, result.group(2).length),
      result.group(4),
      result.group(6)
    )
  }


}
