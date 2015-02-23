require 'narray'

module Spark
  module Mllib
    class BaseVector < ::NArray

      def initialize(stype, *args)
binding.pry unless $__binding
        case stype.to_s.downcase

        when 'dense'
          values = args.shift
          dtype = args.shift
          super(dtype, values.size)
          self[] = values

        when 'sparse'
          size = args.shift
          values = args.shift
          dtype = args.shift
          super(dtype, size)
          fill!(0)

        else
          raise ArgumentError, 'Vector must be Dense or Sparse.'
        end
      end

      def values
        to_a
      end

    end
  end
end
