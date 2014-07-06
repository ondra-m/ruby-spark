require "java"
require "java/spark.jar"
# require "java/spark-ruby.jar"

$CLASSPATH << "java"

java_import org.apache.spark.SparkConf
java_import org.apache.spark.api.java.JavaSparkContext
java_import org.apache.spark.api.ruby.RubyRDD

require "lib/spark/context"
require "lib/spark/rdd"
