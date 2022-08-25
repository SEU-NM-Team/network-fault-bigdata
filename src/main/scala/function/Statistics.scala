package edu.cose.seu
package function


import edu.cose.seu.config.SparkConfig
import edu.cose.seu.util.{CSVUtil, JDBCUtil}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, Row}


object Statistics {

  val schema: StructType = StructType(Array(
    StructField("fault_id", IntegerType, nullable = true),
    StructField("fault_type", StringType, nullable = true),
    StructField("acs_way", StringType, nullable = true),
    StructField("fault_1", StringType, nullable = true),
    StructField("fault_2", StringType, nullable = true),
    StructField("province", StringType, nullable = true),
    StructField("city", StringType, nullable = true),
    StructField("county", StringType, nullable = true),
    StructField("town", StringType, nullable = true),
    StructField("detail", StringType, nullable = true),
    StructField("num", IntegerType, nullable = true),
    StructField("insertTime", TimestampType, nullable = true)
  ))

  /**
   * 从数据库获取故障信息
   */
  val fault_data: DataFrame = CSVUtil.read(schema, "F:\\Java\\network-fault-bigdata\\fault_data\\fault_data.csv")


  def main(args: Array[String]): Unit = {
    /**
     * 故障数=某县区同类型故障的总和
     */
    val faultDF = fault_data
      .groupBy("province", "city", "county", "fault_1", "fault_2")
      .sum("num")
      .as("fault_num")

    /**
     * 重障：某街道故障数>=2即发生了重障
     * 重障数=统计某县区所有发生重障的街道的重障数量，重障数量=num-1
     */
    val againDF = fault_data
      .filter("num > 1")
      .withColumn("num", col("num") - 1)
      .groupBy("province", "city", "county", "fault_1", "fault_2")
      .sum("num")
      .as("again_num")

    faultDF.join(againDF, Seq("province", "city", "county", "fault_1", "fault_2"), "left").show()

    //    faultDF.printSchema()

    //    againDF.printSchema()
  }
}
