package org.apache.spark.api.ruby

import org.apache.spark.util._
import org.apache.spark.{SparkConf, Logging}

object RubyUtils extends Logging {

  def loadPropertiesFile(conf: SparkConf, path: String): String = {
    Utils.getPropertiesFromFile(path).foreach {
      case (key, value) => conf.set(key, value)
    }
    path
  }

}
