require 'matrix'

module Spark
  module Mllib
    class BaseVector

      def initialize(stype, *args)
        case stype.to_s.downcase
        when 'dense'
          init_as_dense(*args)
        when 'sparse'
          init_as_sparse(*args)
        else
          raise ArgumentError, 'Vector must be Dense or Sparse.'
        end
      end

      def [](index)
        super(0, index)
      end

      def size
        column_size
      end

      def init_as_dense(*args)
        values = args.shift
        @__vector__ = Matrix.row_vector(values)
      end

      def init_as_sparse(*args)
        size = args.shift
        values = args.shift
        @__vector__ = Matrix.zero(1, size)
      end

      def values
        to_a.first
      end

      # NArray is initialized on `self.new` instead of `initialize`
      def method_missing(method, *args, &block)
        @__vector__.__send__(method, *args, &block)
      end

    end
  end
end

