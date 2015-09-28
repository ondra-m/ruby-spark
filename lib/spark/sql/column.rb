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

      def !
        func_op('negate')
      end

      def ~
        func_op('not')
      end

      def +(item)
        bin_op('plus', item)
      end

      def -(item)
        bin_op('minus', item)
      end

      def *(item)
        bin_op('multiply', item)
      end

      def /(item)
        bin_op('divide', item)
      end

      def %(item)
        bin_op('mod', item)
      end

      def ==(item)
        bin_op('equalTo', item)
      end

      def !=(item)
        bin_op('notEqual', item)
      end

      def <(item)
        bin_op('lt', item)
      end

      def <=(item)
        bin_op('leq', item)
      end

      def >(item)
        bin_op('gt', item)
      end

      def >=(item)
        bin_op('geq', item)
      end

      def &(item)
        bin_op('and', item)
      end

      def |(item)
        bin_op('or', item)
      end

      # Returns this column aliased with a new name or names (in the case of expressions that
      # return more than one column, such as explode).
      #
      # == Example:
      #   df.select(df.age.alias("age2")).collec
      #   # => [#<Row {"age2"=>2}>, #<Row {"age2"=>5}>]
      #
      def alias(name)
        Column.new(jcolumn.as(name))
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

      def func_op(name)
        new_jcolumn = JSQLFunctions.__send__(name, jcolumn)
        Column.new(new_jcolumn)
      end

    end
  end
end
