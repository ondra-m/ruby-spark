require 'matrix'

# Based on ruby 2.1

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
          super(args.shift.dup)
        when :sparse
          super([0.0] * args.shift.to_i)
        else
          raise Spark::MllibError, 'Unknow vector type.'
        end
      end

      def []=(index, value)
        @elements[index] = value
      end

      def squared_distance(other)
        diff = self - other
        diff.dot(diff)
      end

      def values
        @values || to_a
      end

      alias_method :dot, :inner_product

    end
  end
end
