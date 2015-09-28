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

    end
  end
end
