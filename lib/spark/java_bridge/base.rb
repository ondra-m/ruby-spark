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
        :JStorageLevel => 'org.apache.spark.storage.StorageLevel',
        :JDenseVector => 'org.apache.spark.mllib.linalg.DenseVector'
      ]

      RUBY_TO_JAVA_SKIP = ['Fixnum', 'Integer']

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

      # Call java object
      def call(klass, method, *args)
        # To java
        args.map!{|item| ruby_to_java(item)}

        # Call java
        result = klass.__send__(method, *args)

        # To ruby
        java_to_ruby(result)
      end

      def to_java_array_list(array)
        array_list = ArrayList.new
        array.each do |item|
          array_list.add(ruby_to_java(item))
        end
        array_list
      end

      def ruby_to_java(object)
        if RUBY_TO_JAVA_SKIP.include?(object.class.name)
          # Some object are convert automatically
          # This is for preventing errors
          # For example: jruby store integer as long so 1.to_java is Long
          object
        elsif object.respond_to?(:to_java)
          object.to_java
        elsif object.is_a?(Array)
          to_java_array_list(object)
        else
          object
        end
      end

      def java_to_ruby(result)
        if java_object?(result)

          case result.getClass.name
          when 'scala.collection.convert.Wrappers$SeqWrapper'
            # Rjb:   result.toArray -> Array
            # Jruby: result.toArray -> java.lang.Object
            result.toArray.to_a.map!{|item| java_to_ruby(item)}
          when 'org.apache.spark.mllib.linalg.DenseVector'
            # Values are double
            # and will be automatically converted to floats
            Spark::Mllib::DenseVector.new(result.values)
          end

        else
          # Already transfered
          result
        end
      end

    end
  end
end
