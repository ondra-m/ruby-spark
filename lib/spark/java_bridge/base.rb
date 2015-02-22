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
        'org.apache.spark.api.ruby.RubySerializer',
        'org.apache.spark.api.python.PythonRDD',
        'org.apache.spark.api.python.PythonPartitioner',
        'org.apache.spark.ui.ruby.RubyTab',
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

      # Transfer ruby to java objects
      # Call java
      # Transfer java objects back to ruby
      def call(klass, method, init, *args)
        args.map!{|item| ruby_to_java(item)}

        if init
          klass = klass.new
        end
        klass.__send__(method, *args)
      end

      def to_java_array_list(array)
        array_list = ArrayList.new
        array.each do |item|
          array_list.add(ruby_to_java(item))
        end
        array_list
      end

      def ruby_to_java(object)
        if object.is_a?(Spark::RDD)
          object.to_java
        elsif object.is_a?(Array)
          to_java_array_list(object)
        else
          object
        end
      end

    end
  end
end
