#spark_jar = ENV["SPARK_JAR"] || "java/spark.jar"

require "java"
#require spark_jar

java_import org.apache.spark.SparkConf
java_import org.apache.spark.api.java.JavaSparkContext
java_import org.apache.spark.api.ruby.RubyRDD

require "lib/spark/context"
require "lib/spark/rdd"
