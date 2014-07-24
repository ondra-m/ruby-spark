require "spark/version"
require "spark/error"

module Spark
  autoload :Context,    "spark/context"
  autoload :RDD,        "spark/rdd"
  autoload :CLI,        "spark/cli"
  autoload :Build,      "spark/build"
  autoload :Serializer, "spark/serializer"
  autoload :Command,    "spark/command"

  # Cannot load before CLI::install
  def self.load_lib(spark_home=nil)
    return if @loaded_lib

    spark_home ||= Spark.target_dir

    require "java"
    Dir.glob(File.join(spark_home, "*.jar")){|file| 
      require file
    }
    require Spark.ruby_spark_jar

    java_import org.apache.spark.SparkConf
    java_import org.apache.spark.api.java.JavaSparkContext
    java_import org.apache.spark.api.ruby.RubyRDD
    java_import org.apache.spark.api.python.PairwiseRDD
    java_import org.apache.spark.api.python.PythonPartitioner # for PairwiseRDD

    @loaded_lib = true
  end

  def self.print_logo(message=nil)
    puts <<-STRING

    Welcome to
       ___       ____              __
      |   \\     / __/__  ___ _____/ /__
      | __/    _\\ \\/ _ \\/ _ `/ __/  '_/
      | \\\\    /__ / .__/\\_,_/_/ /_/\\_\\   version #{Spark::VERSION}
      |  \\\\      /_/

    #{message}

    STRING
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
