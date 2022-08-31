package edu.cose.seu
package function

import util.CSVUtil

import edu.cose.seu.config.SparkConfig.field
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.col

/**
 * 生成用户画像
 */
object Portrait {

  val sourceDF: DataFrame = CSVUtil.read(field("file.source_output"))

  def generatePortrait(): Unit = {
    val targetDF = sourceDF.select("user_id", "user_number", "province", "detail", "mood")
      .filter("mood!='无'")
      .withColumn("id",
        org.apache.spark.sql.functions.row_number().over(Window.partitionBy().orderBy(col("province"))))

    CSVUtil.write(targetDF, field("file.portrait_output"))
  }

  def main(args: Array[String]): Unit = {
    generatePortrait()
  }

}
