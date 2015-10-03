##
# Spark::JavaBridge::Base
#
# Parent for all adapter (ruby - java)
#
module Spark
  module JavaBridge
    class Base

      include Spark::Helper::System

      JAVA_OBJECTS = [
        'java.util.ArrayList',
        'scala.collection.mutable.HashMap',
        'org.apache.spark.SparkConf',
        'org.apache.spark.api.java.JavaSparkContext',
        'org.apache.spark.api.ruby.RubyRDD',
        'org.apache.spark.api.ruby.RubyUtils',
        'org.apache.spark.api.ruby.RubyWorker',
        'org.apache.spark.api.ruby.PairwiseRDD',
        'org.apache.spark.api.ruby.RubyAccumulatorParam',
        'org.apache.spark.api.ruby.RubySerializer',
        'org.apache.spark.api.python.PythonRDD',
        'org.apache.spark.api.python.PythonPartitioner',
        'org.apache.spark.api.python.PythonUtils',
        'org.apache.spark.ui.ruby.RubyTab',
        'org.apache.spark.mllib.api.ruby.RubyMLLibAPI',
        :JInteger  => 'java.lang.Integer',
        :JLong     => 'java.lang.Long',
        :JLogger   => 'org.apache.log4j.Logger',
        :JLevel    => 'org.apache.log4j.Level',
        :JPriority => 'org.apache.log4j.Priority',
        :JUtils    => 'org.apache.spark.util.Utils',
        :JDataType => 'org.apache.spark.sql.types.DataType',
        :JSQLContext => 'org.apache.spark.sql.SQLContext',
        :JDenseVector => 'org.apache.spark.mllib.linalg.DenseVector',
        :JDenseMatrix => 'org.apache.spark.mllib.linalg.DenseMatrix',
        :JStorageLevel => 'org.apache.spark.storage.StorageLevel',
        :JSQLFunctions => 'org.apache.spark.sql.functions'
      ]

      JAVA_TEST_OBJECTS = [
        'org.apache.spark.mllib.api.ruby.RubyMLLibUtilAPI'
      ]

      RUBY_TO_JAVA_SKIP = [Fixnum, Integer]

      def initialize(target)
        @target = target
      end

      # Import all important classes into Objects
      def import_all
        return if @imported

        java_objects.each do |name, klass|
          import(name, klass)
        end

        @imported = true
        nil
      end

      # Import classes for testing
      def import_all_test
        return if @imported_test

        java_test_objects.each do |name, klass|
          import(name, klass)
        end

        @imported_test = true
        nil
      end

      # Call java object
      def call(klass, method, *args)
        # To java
        args.map!{|item| to_java(item)}

        # Call java
        result = klass.__send__(method, *args)

        # To ruby
        to_ruby(result)
      end

      def to_array_list(array)
        array_list = ArrayList.new
        array.each do |item|
          array_list.add(to_java(item))
        end
        array_list
      end

      def to_seq(array)
        PythonUtils.toSeq(to_array_list(array))
      end

      def to_long(number)
        return nil if number.nil?
        JLong.new(number)
      end

      def to_java(object)
        if RUBY_TO_JAVA_SKIP.include?(object.class)
          # Some object are convert automatically
          # This is for preventing errors
          # For example: jruby store integer as long so 1.to_java is Long
          object
        elsif object.respond_to?(:to_java)
          object.to_java
        elsif object.is_a?(Array)
          to_array_list(object)
        else
          object
        end
      end

      # Array problem:
      #   Rjb:   object.toArray -> Array
      #   Jruby: object.toArray -> java.lang.Object
      #
      def to_ruby(object)
        # Java object
        if java_object?(object)
          class_name = object.getClass.getSimpleName
          case class_name
          when 'ArraySeq'
            result = []
            iterator = object.iterator
            while iterator.hasNext
              result << to_ruby(iterator.next)
            end
            result
          when 'Map2', 'Map3', 'Map4', 'HashTrieMap'
            Hash[
              object.toSeq.array.to_a.map!{|item| [item._1, item._2]}
            ]
          when 'SeqWrapper'; object.toArray.to_a.map!{|item| to_ruby(item)}
          when 'ofRef';      object.array.to_a.map!{|item| to_ruby(item)} # WrappedArray$ofRef
          when 'LabeledPoint'; Spark::Mllib::LabeledPoint.from_java(object)
          when 'DenseVector';  Spark::Mllib::DenseVector.from_java(object)
          when 'KMeansModel';  Spark::Mllib::KMeansModel.from_java(object)
          when 'DenseMatrix';  Spark::Mllib::DenseMatrix.from_java(object)
          when 'GenericRowWithSchema'; Spark::SQL::Row.from_java(object, true)
          else
            # Some RDD
            if class_name != 'JavaRDD' && class_name.end_with?('RDD')
              object = object.toJavaRDD
              class_name = 'JavaRDD'
            end

            # JavaRDD
            if class_name == 'JavaRDD'
              jrdd = RubyRDD.toRuby(object)

              serializer = Spark::Serializer.build { __batched__(__marshal__) }
              serializer = Spark::Serializer.build { __batched__(__marshal__, 2) }

              return Spark::RDD.new(jrdd, Spark.sc, serializer, deserializer)
            end

            # Unknow
            Spark.logger.warn("Java object '#{object.getClass.name}' was not converted.")
            object
          end

        # Array can be automatically transfered but content not
        elsif object.is_a?(Array)
          object.map! do |item|
            to_ruby(item)
          end
          object

        # Already transfered
        else
          object
        end
      end

      alias_method :java_to_ruby, :to_ruby
      alias_method :ruby_to_java, :to_java

      private

        def jars
          result = Dir.glob(File.join(@target, '*.jar'))
          result.flatten!
          result
        end

        def objects_with_names(objects)
          hash = {}
          objects.each do |object|
            if object.is_a?(Hash)
              hash.merge!(object)
            else
              key = object.split('.').last.to_sym
              hash[key] = object
            end
          end
          hash
        end

        def java_objects
          objects_with_names(JAVA_OBJECTS)
        end

        def java_test_objects
          objects_with_names(JAVA_TEST_OBJECTS)
        end

        def raise_missing_class(klass)
          raise Spark::JavaBridgeError, "Class #{klass} is missing. Make sure that Spark and RubySpark is assembled."
        end

    end
  end
end
