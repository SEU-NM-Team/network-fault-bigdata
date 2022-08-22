package edu.cose.seu
package function

import util.CSVUtil

import edu.cose.seu.config.SparkConfig
import org.apache.spark.sql.DataFrame

object Pretreatment {
  val sourceDF: DataFrame = CSVUtil.read(SparkConfig.field("file.path"))

  var targetDF: DataFrame = sourceDF.na.fill("未知", Seq("fault_2"))

  targetDF = targetDF
    .filter("sheng is not null")
    .filter("shi is not null")
    .filter("address is not null")
    .filter("shi != '派单正确'")
    .filter("length(fault_2)<25")


  def main(args: Array[String]): Unit = {
    CSVUtil.write(targetDF.groupBy("fault_2").count(), "filter_5")
  }


}