module Spark
  module JavaBridge
    class Base

      include Spark::Helper::System

      JAVA_OBJECTS = [
        'java.util.ArrayList',
        'org.apache.spark.SparkConf',
        'org.apache.spark.api.java.JavaSparkContext',
        'org.apache.spark.api.ruby.RubyRDD',
        'org.apache.spark.api.ruby.RubyWorker',
        'org.apache.spark.api.ruby.PairwiseRDD',
        'org.apache.spark.api.ruby.RubyAccumulatorParam',
        'org.apache.spark.ui.ruby.RubyTab',
        'org.apache.spark.api.python.PythonRDD',
        'org.apache.spark.api.python.PythonPartitioner',
        'org.apache.spark.mllib.api.ruby.RubyMLLibAPI',
        'scala.collection.mutable.HashMap',
        :JInteger  => 'java.lang.Integer',
        :JLogger   => 'org.apache.log4j.Logger',
        :JLevel    => 'org.apache.log4j.Level',
        :JPriority => 'org.apache.log4j.Priority',
        :JUtils    => 'org.apache.spark.util.Utils',
        :JStorageLevel => 'org.apache.spark.storage.StorageLevel'
      ]

      def initialize(spark_home)
        @spark_home = spark_home
      end

      def jars
        result = []
        if File.file?(@spark_home)
          result << @spark_home
        else
          result << Dir.glob(File.join(@spark_home, '*.jar'))
        end
        result << Spark.ruby_spark_jar
        result.flatten
      end

      def java_objects
        hash = {}
        JAVA_OBJECTS.each do |object|
          if object.is_a?(Hash)
            hash.merge!(object)
          else
            key = object.split('.').last.to_sym
            hash[key] = object
          end
        end
        hash
      end

    end
  end
end
