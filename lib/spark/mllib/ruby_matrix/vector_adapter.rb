require 'matrix'

# Based on ruby 2.1

class Vector
  def self.elements(array, copy=true)
    DenseVector.new(convert_to_array(array, copy))
  end
end

module Spark
  module Mllib
    class VectorAdapter < ::Vector

      def self.new(*args)
        object = self.allocate
        object.__send__(:initialize, *args)
        object
      end

      def initialize(*args)
        case args.shift
        when :dense
          values = args.shift.dup
        when :sparse
          values = [0.0] * args.shift.to_i
        else
          raise Spark::MllibError, 'Unknow vector type.'
        end

        super(values)
      end

      def []=(index, value)
        @elements[index] = value
      end

      def dot(other)
        if other.is_a?(Spark::Mllib::MatrixBase)
          other * self
        else
          inner_product(other)
        end
      end

      def squared_distance(other)
        diff = self - other
        diff.dot(diff)
      end

      def values
        @values || to_a
      end

    end
  end
end
