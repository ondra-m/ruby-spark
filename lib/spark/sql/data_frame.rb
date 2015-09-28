module Spark
  module SQL
    class DataFrame

      attr_reader :jdf, :sql_context

      def initialize(jdf, sql_context)
        @jdf = jdf
        @sql_context = sql_context
      end

      def show_string(n=20, truncate=true)
        jdf.showString(n, truncate)
      end

      # Prints the first n rows to the console.
      #
      # == Parameters:
      # n:: Number of rows to show.
      # truncate:: Whether truncate long strings and align cells right.
      #
      def show(n=20, truncate=true)
        puts show_string(n, truncate)
      end

      # Prints out the schema in the tree format.
      #
      # == Example:
      #   df.print_schema
      #   root
      #    |-- age: integer (nullable = true)
      #    |-- name: string (nullable = true)
      #
      def print_schema
        puts jdf.schema.treeString
      end

    end
  end
end
