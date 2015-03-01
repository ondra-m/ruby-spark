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

      def initialize(values)
        super('dense', values.to_a)
      end

      def to_java
        JDenseVector.new(values)
      end

      def marshal_dump
        values
      end

      def marshal_load(array)
        raise NotImplementedError, 'marshal_load'
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

      def initialize(size, indices_and_values)
        super('sparse', size)

        if indices_and_values.is_a?(Hash)
          @indices = indices_and_values.keys
          @_values = indices_and_values.values
        else
          @indices = indices_and_values[0]
          @_values = indices_and_values[1]
        end

        @_values.map!(&:to_f)

        @indices.zip(@_values).each do |(index, value)|
          self[index] = value
        end
      end

      # Vectors can have own values
      def values
        @_values
      end

      def marshal_dump
        [size, indices, values]
      end

      def marshal_load(array)
        raise NotImplementedError, 'marshal_load'
      end

    end
  end
end
