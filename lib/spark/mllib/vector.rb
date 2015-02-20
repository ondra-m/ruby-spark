require 'nmatrix'

module Spark
  module Mllib
    class Vector < ::NMatrix
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
      def initialize(array)
        super([array.size], array.to_a, stype: :dense)
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
        end
      end

      def dot(other)

      end
    end
  end
end
