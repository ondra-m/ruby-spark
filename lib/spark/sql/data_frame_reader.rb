module Spark
  module SQL
    class DataFrameReader

      attr_reader :sql_context, :jreader

      def initialize(sql_context)
        @sql_context = sql_context
        @jreader = sql_context.jsql_context.read
      end

      def df(jdf)
        DataFrame.new(jdf, sql_context)
      end

      # Loads a JSON file (one object per line) and returns the result as {DataFrame}
      #
      # If the schema parameter is not specified, this function goes
      # through the input once to determine the input schema.
      #
      # == Parameters:
      # path:: string, path to the JSON dataset
      # schema:: an optional {StructType} for the input schema.
      #
      # == Example:
      #   df = sql.read.json('people.json')
      #   df.dtypes
      #   # => [('age', 'bigint'), ('name', 'string')]
      #
      def json(path, new_schema=nil)
        if new_schema
          schema(new_schema)
        end

        # ClassNotFoundException: Failed to load class for data source: json
        # df(jreader.json(path))

        df(jreader.format('org.apache.spark.sql.execution.datasources.json').load(path))
      end

    end
  end
end
