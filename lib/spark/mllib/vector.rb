module Spark
  module Mllib
    class Vector < BaseVector

      include Spark::Helper::Serialize

      def self.to_vector(data)
        if data.is_a?(SparseVector) || data.is_a?(DenseVector)
          data
        elsif data.is_a?(Array)
          DenseVector.new(data)
        end
      end

    end
  end
end

##
# A dense vector represented by a value array.
#
# Dense vector is a vector in which most of the elements are non-zero.
#
# == Example:
#   DenseVector.new([1,2,3,4,5])
#   # => [1, 2, 3, 4, 5]
#
#   DenseVector.new(1..5)
#   # => [1, 2, 3, 4, 5]
#
module Spark
  module Mllib
    class DenseVector < Vector

      def initialize(values, dtype: 'float')
        super('dense', values.to_a, dtype)
      end

      # TODO: move method to C/Java
      #       send smaller string
      #       use compress
      def _dump
        result = 'd'
        result << pack_int(size)
        result << pack_doubles(values)
        result.encode(Encoding::ASCII_8BIT)
      end

      def to_java
        JDenseVector.new(values)
      end

    end
  end
end

##
# A sparse vector represented by an index array and an value array.
#
# Sparse vector is a vector in which most of the elements are zero.
#
# == Example:
#   SparseVector.new(4, {1 => 1.0, 3 => 5.5})
#   # => [0, 1.0, 0, 5.5]
#
#   SparseVector.new(4, [[1, 3], [1.0, 5.5]])
#   # => [0, 1.0, 0, 5.5]
#
module Spark
  module Mllib
    class SparseVector < Vector

      attr_reader :indices
      attr_reader :values

      def initialize(size, values, dtype: 'float')
        super('sparse', size, values, dtype)

        if values.is_a?(Hash)
          @indices = values.keys
          @values = values.values
        else
          @indices = values[0]
          @values = values[1]
        end

        @indices.zip(@values).each do |(index, value)|
          self[index] = value
        end
      end

      # TODO: move method to C/Java
      #       send smaller string
      #       use compress
      def _dump
        result = 's'
        result << pack_int(size)
        result << pack_int(indices.size)
        result << pack_ints(indices)
        result << pack_int(values.size)
        result << pack_doubles(values)
        result.encode(Encoding::ASCII_8BIT)
      end

    end
  end
end
