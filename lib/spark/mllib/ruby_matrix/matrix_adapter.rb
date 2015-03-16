require 'matrix'

module Spark
  module Mllib
    class MatrixAdapter < ::Matrix

      def self.new(*args)
        object = self.allocate

        if args.size == 2
          # Matrix is initialized from Matrix
          # Arguments: rows, column count
          object.__send__(:original_initialize, *args)
        else
          object.__send__(:initialize, *args)
        end

        object
      end

      alias_method :original_initialize, :initialize

      def initialize(type, rows, cols, values=nil)
        case type
        when :dense
          values = values.dup
          if rows * cols == values.size
            # Values are on one row
            # 2x2 => [1,2,3,4]
            values = values.each_slice(cols).to_a
          else
            # 2x2 => [[1,2], [3,4]]
          end
        when :sparse
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
