module Spark
  module SQL
    class Column

      def self.to_java(col)
        if col.is_a?(Column)
          col.jcolumn
        else
          JSQLFunctions.col(col)
        end
      end

      attr_reader :jcolumn

      def initialize(jcolumn)
        @jcolumn = jcolumn
      end

      def +(item)
        bin_op('plus', item)
      end




      def bin_op(name, item)
        if item.is_a?(Column)
          col = item.jcolumn
        else
          col = item
        end

        new_jcolumn = jcolumn.__send__(name, col)
        Column.new(new_jcolumn)
      end

    end
  end
end
