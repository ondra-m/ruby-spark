require 'matrix'

module Spark
  module Mllib
    class MatrixAdapter < ::Matrix

      def self.new(*args)
        object = self.allocate
        object.__send__(:initialize, *args)
        object
      end

      def initialize(*args)
        case args.shift
        when :dense
          rows = args.shift
          cols = args.shift
          values = args.shift.dup
        when :sparse
          rows = args.shift
          cols = args.shift
          values = Array.new(rows) { Array.new(cols) { 0.0 } }
        else
          raise Spark::MllibError, 'Unknow vector type.'
        end

        super(values, cols)
      end

      def shape
        [row_count, column_count]
      end

      def values
        @values || to_a
      end

    end
  end
end
