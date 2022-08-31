package edu.cose.seu
package rawdata

import config.SparkConfig.field
import util.{CSVUtil, JDBCUtil}

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, length}
import org.apache.spark.sql.types.StructType

object RawPretreatment {

  /**
   * 原始数据处理
   * <p>1.去除空行</p>
   * <p>2.去除特定字段为空的数据</p>
   * <p>3.去除id重复的数据</p>
   * <p>4.缺失值处理</p>
   * <p>5.异常值处理</p>
   *
   * @return cleanDF:DataFrame
   */
  def rawPretreatment(): DataFrame = {
    /**
     * 读取由txt生成的csv文件
     */
    val srcDF: DataFrame = CSVUtil.read(field("file.workdata_csv"))

    /**
     * 去除空行
     */
    var targetDF = srcDF.na.drop("all")

    /**
     * 去除id为空、地址为空、用户id为空、故障时间为空的数据
     */
    targetDF = targetDF.na.drop(Seq("fault_id", "province", "city", "user_id", "fault_time"))

    /**
     * 去除id重复的数据
     */
    targetDF = targetDF.dropDuplicates("fault_id");

    /**
     * 缺失值处理
     */
    targetDF = targetDF.na.fill(Map(
      "user_number" -> "未预留电话",
      "value1" -> 0,
      "value2" -> 0,
      "value3" -> 0,
      "mood" -> "无",
      "service" -> "未标注",
      "service_detail" -> "未标注",
      "fault" -> "未知错误",
      "fault_type" -> "未知类型",
      "acs_way" -> "未知接入方式",
      "detail" -> "未知地址"
    ))

    /**
     * 去除仍然包含null字段的数据
     */
    targetDF = targetDF.na.drop()

    /**
     * 异常值处理
     */
    targetDF = targetDF.filter(col("acs_way").rlike(field("dict.acsFilter")))
      .filter(length(col("province")) < 3)
      .filter(length(col("city")) < 11)

    /**
     * 写入CSV，JDBC
     */
    CSVUtil.write(targetDF, field("file.source_output"))
    //    JDBCUtil.writeTable(targetDF, "source_data", "append", field("jdbc.url"))

    return targetDF
  }


  def main(args: Array[String]): Unit = {
    rawPretreatment()
  }

}
