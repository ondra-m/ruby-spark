module Spark
  module JavaBridge
    module Converter

      @@registered = {}

      def self.register(klass, *names)
        names.each do |name|
          if @@registered.has_key?(name)
            raise Spark::JavaBridgeError, "Name '#{name}' already exist."
          end

          @@registered[name] = klass.new
        end
      end

      def self.by_name(name)
        @@registered[name.to_s]
      end

    end
  end
end

module Spark
  module JavaBridge
    module Converter

      class Base
        def self.register(*names)
          Spark::JavaBridge::Converter.register(self, *names)
        end
      end

      class SeqWraper < Base
        register 'scala.collection.convert.Wrappers$SeqWrapper'

        def to_ruby(object)
          object.toArray.to_a.map!{|item| java_to_ruby(item)}
        end
      end

      class WrappedArrayOfRef < Base
        register 'scala.collection.mutable.WrappedArray$ofRef'

        def to_ruby(object)
          object.array.to_a.map!{|item| java_to_ruby(item)}
        end
      end

      class LabeledPoint < Base
        register 'org.apache.spark.mllib.regression.LabeledPoint'

        def to_ruby(object)
          Spark::Mllib::LabeledPoint.from_java(object)
        end
      end

      class DenseVector < Base
        register 'org.apache.spark.mllib.linalg.DenseVector'

        def to_ruby(object)
          Spark::Mllib::DenseVector.from_java(object)
        end
      end

      class ArraySeq < Base
        register 'scala.collection.mutable.ArraySeq'

        def to_ruby(object)
          result = []
          iterator = object.iterator
          while iterator.hasNext
            result << java_to_ruby(iterator.next)
          end
          result
        end
      end

      class KMeansModel < Base
        register 'org.apache.spark.mllib.clustering.KMeansModel'

        def to_ruby(object)
          Spark::Mllib::KMeansModel.from_java(object)
        end
      end

      class DenseMatrix < Base
        register 'org.apache.spark.mllib.linalg.DenseMatrix'

        def to_ruby(object)
          Spark::Mllib::DenseMatrix.from_java(object)
        end
      end

    end
  end
end
