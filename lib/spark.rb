require "spark/version"

require "java"

java_import org.apache.spark.SparkConf
java_import org.apache.spark.api.java.JavaSparkContext
java_import org.apache.spark.api.ruby.RubyRDD

module Spark
  autoload :Context, "spark/context"
  autoload :RDD,     "spark/rdd"
  autoload :CLI,     "spark/cli"
end
