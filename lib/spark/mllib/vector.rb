require 'nmatrix'

module Spark
  module Mllib
    class Vector < ::NMatrix

      include Spark::Helper::Serialize

      def self.dot(array1, array2)
        if array1.size != array2.size
          raise ArgumentError, "incompatible dimensions"
        end

        result = 0
        array1.size.times do |i|
          result += array1[i] * array2[i]
        end

        return [result]
      end

      def self.init_from(data)
        if data.is_a?(Vector)
          data
        else
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
        super([values.size], values.to_a, stype: :dense)
        @values = values
      end

      # TODO: vector * array
      def dot(other)
        # NMatrix 0.1.0 support dot product only fot same stype
        if other.is_a?(NMatrix) && self.stype == other.stype
          return super.to_a
        end

        if other.is_a?(SparseVector)
          return other.dot(self)
        end

        if other.is_a?(Array)
          return Vector.dot(self, other)
        end

        raise ArgumentError, "Incopatible type #{other.class}. Use NMatrix, Vector or Array."
      end

      # TODO: move method to C/Java
      #       send smaller string
      #       use compress
      def _dump
        result = "d"
        result << pack_int(size)
        result << pack_doubles(@values)
        result
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
#   SparseVector.new(4, [[1, 1.0], [3, 5.5]])
#   # => [0, 1.0, 0, 5.5]
#
#   SparseVector.new(4, [1, 3], [1.0, 5.5])
#   # => [0, 1.0, 0, 5.5]
#
module Spark
  module Mllib
    class SparseVector < Vector

      def initialize(size, locations=nil, values=nil)
        super([size], stype: :list)

        if !locations.nil? && !values.nil?
          locations = Hash[locations.zip(values)]
        elsif locations.is_a?(Array)
          locations = Hash[locations]
        end

        if locations.is_a?(Hash)
          locations.each do |index, value|
            self[index.to_i] = value
          end

          @indices = locations.keys
          @values = locations.values
        end
      end

      # TODO: vector * array
      def dot(other)
        # NMatrix 0.1.0 support dot product only fot same stype
        if other.is_a?(NMatrix) && self.stype == other.stype
          return super.to_a
        end

        if other.is_a?(DenseVector)
          return Vector.dot(self, other)
        end

        if other.is_a?(Array)
          return Vector.dot(self, other)
        end

        raise ArgumentError, "Incopatible type #{other.class}. Use NMatrix, Vector or Array."
      end

      # TODO: move method to C/Java
      #       send smaller string
      #       use compress
      def _dump
        result = "s"
        result << pack_int(size)
        result << pack_int(@indices.size)
        result << pack_ints(@indices)
        result << pack_int(@values.size)
        result << pack_doubles(@values)
        result
      end

    end
  end
end
