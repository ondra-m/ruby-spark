require 'narray'

module Spark
  module Mllib
    class BaseMatrix

      attr_reader :matrix

      def initialize(stype, *args)
        case stype.to_s.downcase
        when 'dense'
          init_as_dense(*args)
        when 'sparse'
          init_as_sparse(*args)
        else
          raise ArgumentError, 'Matrix must be Dense or Sparse.'
        end
      end

      def init_as_dense(*args)
        rows = args.shift
        cols = args.shift
        values = args.shift
        @matrix = NArray.float(cols, rows)
        self[] = values
      end

      def init_as_sparse(*args)
        rows = args.shift
        cols = args.shift
        @matrix = NArray.float(cols, rows)
        @matrix.fill!(0)
      end

      # Spark access to element via row:column
      # NArray column:row
      def [](*args)
        if args.size == 2
          @matrix[args[1], args[0]]
        else
          @matrix[*args]
        end
      end

      def []=(*args)
        if args.size == 3
          @matrix[args[1], args[0]] = args[2]
        else
          @matrix.[]=(*args)
        end
      end

      def shape
        @matrix.shape.reverse
      end

      def values
        @matrix.to_a
      end

      # NArray is initialized on `self.new` instead of `initialize`
      def method_missing(method, *args, &block)
        args.map! do |arg|
          if arg.is_a?(BaseMatrix)
            arg.matrix
          else
            arg
          end
        end

        @matrix.__send__(method, *args, &block)
      end

    end
  end
end
