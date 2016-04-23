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

      # Specifies the input data source format.
      # Parameter is name of the data source, e.g. 'json', 'parquet'.
      def format(source)
        jreader.format(source)
        self
      end

      # Adds an input option for the underlying data source.
      def option(key, value)
        jreader.option(key, value.to_s)
        self
      end

      # Adds input options for the underlying data source.
      def options(options)
        options.each do |key, value|
          jreader.option(key, value.to_s)
        end
        self
      end

      # Loads data from a data source and returns it as a :class`DataFrame`.
      #
      # == Parameters:
      # path:: Optional string for file-system backed data sources.
      # format:: Optional string for format of the data source. Default to 'parquet'.
      # schema:: Optional {StructType} for the input schema.
      # options:: All other string options.
      #
      def load(path=nil, new_format=nil, new_schema=nil, new_options=nil)
        new_format && format(new_format)
        new_schema && schema(new_schema)
        new_options && options(new_options)

        if path.nil?
          df(jreader.load)
        else
          df(jreader.load(path))
        end
      end

      # Specifies the input schema.
      #
      # Some data sources (e.g. JSON) can infer the input schema automatically from data.
      # By specifying the schema here, the underlying data source can skip the schema
      # inference step, and thus speed up data loading.
      #
      # Parameter schema must be StructType object.
      #
      def schema(new_schema)
        unless new_schema.is_a?(StructType)
          raise ArgumentError, 'Schema must be a StructType.'
        end

        jschema = sql_context.jsql_context.parseDataType(new_schema.json)
        jreader.schema(jschema)
        self
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
        # ClassNotFoundException: Failed to load class for data source: json
        # df(jreader.json(path))

        load(path, 'org.apache.spark.sql.execution.datasources.json', new_schema)
      end

    end
  end
end
