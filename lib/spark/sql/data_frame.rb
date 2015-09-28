module Spark
  module SQL
    ##
    # Spark::SQL::DataFrame
    #
    # All example are base on people.json
    #
    class DataFrame

      attr_reader :jdf, :sql_context

      def initialize(jdf, sql_context)
        @jdf = jdf
        @sql_context = sql_context
      end

      # Returns the column as a {Column}.
      def [](item)
        case item
        when String
          jcolumn = jdf.apply(item)
          Column.new(jcolumn)
        else
          raise ArgumentError, "Unsupported type: #{item.class}"
        end
      end




      # Returns all column names as a Array.
      #
      # == Example:
      #   df.columns
      #   # => ['age', 'name']
      #
      def columns
        schema.fields.map(&:name)
      end

      # Returns the schema of this {DataFrame} as a {StructType}.
      #
      def schema
        return @schema if @schema

        begin
          @schema = DataType.parse(JSON.parse(jdf.schema.json))
        rescue => e
          raise Spark::ParseError, 'Unable to parse datatype from schema'
        end
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


      # =============================================================================
      # Queries

      # Projects a set of expressions and returns a new {DataFrame}
      #
      # == Parameters:
      # *cols::
      #   List of column names (string) or expressions {Column}.
      #   If one of the column names is '*', that column is expanded to include all columns
      #   in the current DataFrame.
      #
      # == Example:
      #   df.select('*').collect()
      #   [Row(age=2, name=u'Alice'), Row(age=5, name=u'Bob')]
      #   df.select('name', 'age').collect()
      #   [Row(name=u'Alice', age=2), Row(name=u'Bob', age=5)]
      #   df.select(df.name, (df.age + 10).alias('age')).collect()
      #   [Row(name=u'Alice', age=12), Row(name=u'Bob', age=15)]
      #
      def select(*cols)
        jcols = cols.map do |col|
          Column.to_java(col)
        end

        new_jdf = jdf.select(jcols)
        DataFrame.new(new_jdf, sql_context)
      end

    end
  end
end
