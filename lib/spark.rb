require "spark/version"
require "spark/error"

module Spark
  autoload :Context,    "spark/context"
  autoload :RDD,        "spark/rdd"
  autoload :CLI,        "spark/cli"
  autoload :Build,      "spark/build"
  autoload :Serializer, "spark/serializer"

  # Cannot load before CLI::install
  def self.load_lib
    require "java"

    java_import org.apache.spark.SparkConf
    java_import org.apache.spark.api.java.JavaSparkContext
    java_import org.apache.spark.api.ruby.RubyRDD
  end

  def self.root
    @root ||= File.expand_path("..", File.dirname(__FILE__))
  end

  def self.target_dir
    @target_dir ||= File.join(root, 'target')
  end

  def self.worker_dir
    @worker_dir ||= File.join(root, 'lib', 'spark', 'worker')
  end

  def self.ruby_spark_jar
    @ruby_spark_jar ||= File.join(target_dir, 'ruby-spark.jar')
  end

end
