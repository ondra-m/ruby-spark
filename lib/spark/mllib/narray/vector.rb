require 'narray'

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

      def init_as_dense(*args)
        values = args.shift
        dtype = args.shift
        @__vector__ = NArray.new(dtype, values.size)
        self[] = values
      end

      def init_as_sparse(*args)
        size = args.shift
        values = args.shift
        dtype = args.shift
        @__vector__ = NArray.new(dtype, size)
        fill!(0)
      end

      def values
        to_a
      end

      # NArray is initialized on `self.new` instead of `initialize`
      def method_missing(method, *args, &block)
        @__vector__.__send__(method, *args, &block)
      end

    end
  end
end
