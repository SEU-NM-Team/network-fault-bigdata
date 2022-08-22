package edu.cose.seu
package util

import config.SparkConfig
import org.apache.spark.sql.DataFrame

object CSVUtil {

  def read(path:String):DataFrame={
    return SparkConfig.spark.read.option("header","true").csv(path)
  }

  def write(df:DataFrame,path:String):Unit={
    df.coalesce(1).write.option("header", "true").csv(path)
  }
}
