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
      #
      # == Examples:
      #   df.select(df['age']).collect
      #   # => [#<Row {"age"=>2}>, #<Row {"age"=>5}>]
      #
      #   df[ ["name", "age"] ].collect
      #   # => [#<Row {"name"=>"Alice", "age"=>2}>, #<Row {"name"=>"Bob", "age"=>5}>]
      #
      #   df[ df.age > 3 ].collect
      #   # => [#<Row {"age"=>5, "name"=>"Bob"}>]
      #
      #   df[df[0] > 3].collect
      #   # => [#<Row {"age"=>5, "name"=>"Bob"}>]
      #
      def [](item)
        case item
        when String
          jcolumn = jdf.apply(item)
          Column.new(jcolumn)
        when Array
          select(*items)
        when Numeric
          jcolumn = jdf.apply(columns[item])
          Column.new(jcolumn)
        when Column
          where(item)
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
      #   # root
      #   #  |-- age: integer (nullable = true)
      #   #  |-- name: string (nullable = true)
      #
      def print_schema
        puts jdf.schema.treeString
      end

      def explain(extended=false)
        if extended
          jdf.queryExecution.toString
        else
          jdf.queryExecution.executedPlan.toString
        end
      end

      # Prints the (logical and physical) plans to the console for debugging purpose.
      #
      # == Example:
      #   df.print_explain
      #   # Scan PhysicalRDD[age#0,name#1]
      #
      #   df.print_explain(true)
      #   # == Parsed Logical Plan ==
      #   # ...
      #   # == Analyzed Logical Plan ==
      #   # ...
      #   # == Optimized Logical Plan ==
      #   # ...
      #   # == Physical Plan ==
      #   # ...
      #
      def print_explain(extended=false)
        puts explain(extended)
      end

      # Returns all column names and their data types as a list.
      #
      # == Example:
      #   df.dtypes
      #   # => [('age', 'int'), ('name', 'string')]
      #
      def dtypes
        schema.fields.map do |field|
          [field.name, field.data_type.simple_string]
        end
      end

      def inspect
        types = dtypes.map do |(name, type)|
          "#{name}: #{type}"
        end

        "#<DataFrame(#{types.join(', ')})>"
      end

      # Get column by name
      def method_missing(method, *args, &block)
        name = method.to_s
        if columns.include?(name)
          self[name]
        else
          super
        end
      end


      # =============================================================================
      # Collect

      # Returns all the records as a list of {Row}.
      #
      # == Example:
      #   df.collect
      #   # => [Row(age=2, name=u'Alice'), Row(age=5, name=u'Bob')]
      #
      def collect
        Spark.jb.call(jdf, 'collect')
      end

      def collect_as_hash
        result = collect
        result.map!(&:to_h)
        result
      end

      def values
        result = collect
        result.map! do |item|
          item.to_h.values
        end
        result
      end

      # Returns the number of rows in this {DataFrame}.
      def count
        jdf.count.to_i
      end

      # Returns the first num rows as an Array of {Row}.
      def take(num)
        limit(num).collect
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
      #   df.select('*').collect
      #   # => [#<Row {"age"=>2, "name"=>"Alice"}>, #<Row {"age"=>5, "name"=>"Bob"}>]
      #
      #   df.select('name', 'age').collect
      #   # => [#<Row {"name"=>"Alice", "age"=>2}>, #<Row {"name"=>"Bob", "age"=>5}>]
      #
      #   df.select(df.name, (df.age + 10).alias('age')).collect
      #   # => [#<Row {"name"=>"Alice", "age"=>12}>, #<Row {"name"=>"Bob", "age"=>15}>]
      #
      def select(*cols)
        jcols = cols.map do |col|
          Column.to_java(col)
        end

        new_jdf = jdf.select(jcols)
        DataFrame.new(new_jdf, sql_context)
      end

      # Filters rows using the given condition.
      #
      # == Examples:
      #   df.filter(df.age > 3).collect
      #   # => [#<Row {"age"=>5, "name"=>"Bob"}>]
      #
      #   df.where(df.age == 2).collect
      #   # => [#<Row {"age"=>2, "name"=>"Alice"}>]
      #
      #   df.filter("age > 3").collect
      #   # => [#<Row {"age"=>5, "name"=>"Bob"}>]
      #
      #   df.where("age = 2").collect
      #   # => [#<Row {"age"=>2, "name"=>"Alice"}>]
      #
      def filter(condition)
        case condition
        when String
          new_jdf = jdf.filter(condition)
        when Column
          new_jdf = jdf.filter(condition.jcolumn)
        else
          raise ArgumentError, 'Condition must be String or Column'
        end

        DataFrame.new(new_jdf, sql_context)
      end

      # Limits the result count to the number specified.
      def limit(num)
        new_jdf = jdf.limit(mum)
        DataFrame.new(new_jdf, sql_context)
      end


      alias_method :where, :filter

    end
  end
end
