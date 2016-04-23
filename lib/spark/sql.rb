module Spark
  module SQL
    extend Spark::Library

    autoload_without_import :Context,         'spark/sql/context'
    autoload_without_import :DataType,        'spark/sql/data_type'
    autoload_without_import :DataFrame,       'spark/sql/data_frame'
    autoload_without_import :DataFrameReader, 'spark/sql/data_frame_reader'

    autoload :Row,    'spark/sql/row'
    autoload :Column, 'spark/sql/column'

    # Types
    autoload :StructType,   'spark/sql/data_type'
    autoload :StructField,  'spark/sql/data_type'
    autoload :AtomicType,   'spark/sql/data_type'
    autoload :NumericType,  'spark/sql/data_type'
    autoload :IntegralType, 'spark/sql/data_type'
    autoload :StringType,   'spark/sql/data_type'
    autoload :LongType,     'spark/sql/data_type'
  end

  SQLContext = Spark::SQL::Context
end
