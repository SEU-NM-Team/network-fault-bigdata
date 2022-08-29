package edu.cose.seu
package util

import edu.cose.seu.config.SparkConfig.spark
import org.apache.spark.sql.DataFrame

object JSONUtil {

  def readjson(path: String): DataFrame = {
    return spark
      .read
      .option("multiline", "true")
      .json(path)
  }

  def writejson(df: DataFrame, path: String): Unit = {
    df.write.json(path)
  }


}
