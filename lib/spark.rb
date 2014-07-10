require "spark/version"

module Spark
  autoload :Context, "spark/context"
  autoload :RDD,     "spark/rdd"
  autoload :CLI,     "spark/cli"
  autoload :Build,   "spark/build"

  # Cannot load before CLI::install
  def self.load_lib
    require "java"

    java_import org.apache.spark.SparkConf
    java_import org.apache.spark.api.java.JavaSparkContext
    java_import org.apache.spark.api.ruby.RubyRDD
  end

  def self.default_target
    @default_target ||= File.expand_path(File.dirname(__FILE__) + '/../target')
  end

  def self.ruby_worker
    @ruby_worker ||= File.expand_path(File.dirname(__FILE__) + '/spark/worker.rb')
  end

end
