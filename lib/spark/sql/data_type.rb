module Spark
  module SQL
    ##
    # Spark::SQL::DataType
    #
    class DataType

      cattr_accessor :atomic_types
      self.atomic_types = {}

      cattr_accessor :complex_types
      self.complex_types = {}

      def self.parse(data)
        if data.is_a?(Hash)
          type = data['type']
          if complex_types.has_key?(type)
            complex_types[type].from_json(data)
          # elsif type == 'udt'
          #   UserDefinedType.from_json(data)
          else
            raise Spark::SQLError, "Unsupported type: #{type}"
          end
        else
          if atomic_types.has_key?(data)
            atomic_types[data].new
          else
            raise Spark::SQLError, "Unsupported type: #{type}"
          end
        end
      end

      def self.class_name
        name.split('::').last
      end

      def self.type_name
        class_name.sub('Type', '').downcase
      end

      def self.complex
        complex_types[type_name] = self
      end

      def self.atomic
        atomic_types[type_name] = self
      end

      def ==(other)
        self.class == other.class && self.to_s == other.to_s
      end

      def type_name
        self.class.type_name
      end

      def simple_string
        type_name
      end

      def json_value
        type_name
      end

      def json
        json_value.to_json
      end

      def to_s
        self.class.class_name
      end

      def inspect
        "#<#{to_s}>"
      end

    end

    ##
    # Spark::SQL::StructType
    #
    # Struct type, consisting of a list of {StructField}.
    # This is the data type representing a {Row}.
    #
    # == Example:
    #   struct1 = StructType.new([StructField.new('f1', StringType.new, true)])
    #   struct2 = StructType.new([StructField.new('f2', StringType.new, true)])
    #   struct1 == struct2
    #   # => true
    #
    class StructType < DataType
      complex

      attr_reader :fields

      def self.from_json(json)
        fields = json['fields'].map do |field|
          StructField.from_json(field)
        end

        StructType.new(fields)
      end

      def initialize(fields=[])
        @fields = fields
        @names = fields.map(&:name)
      end

      def json_value
        {
          'type' => type_name,
          'fields' => fields.map(&:json_value)
        }
      end

      def to_s
        "StructType(#{fields.join(', ')})"
      end
    end


    ##
    # Spark::SQL::StructField
    #
    class StructField < DataType

      attr_reader :name, :data_type, :nullable, :metadata

      def self.from_json(json)
        StructField.new(json['name'], DataType.parse(json['type']), json['nullable'], json['metadata'])
      end

      # A field in {StructType}.
      #
      # == Parameters:
      # name:: string, name of the field.
      # data_type:: {DataType} of the field.
      # nullable:: boolean, whether the field can be null (nil) or not.
      # metadata:: a dict from string to simple type that can be to_internald to JSON automatically
      #
      # == Example:
      #   f1 = StructField.new('f1', StringType.new, true)
      #   f2 = StructField.new('f2', StringType.new, true)
      #   f1 == f2
      #   # => true
      #
      def initialize(name, data_type, nullable=true, metadata={})
        @name = name
        @data_type = data_type
        @nullable = nullable
        @metadata = metadata
      end

      def json_value
        {
          'name' => name,
          'type' => data_type.json_value,
          'nullable' => nullable,
          'metadata' => metadata,
        }
      end

      def to_s
        %{StructField(#{name}, #{data_type}, #{nullable})}
      end
    end

    ##
    # Spark::SQL::AtomicType
    #
    # An internal type used to represent everything that is not
    # null, UDTs, arrays, structs, and maps.
    #
    class AtomicType < DataType
    end

    ##
    # Spark::SQL::BooleanType
    #
    # Boolean data type.
    #
    class BooleanType < AtomicType
      atomic
    end

    ##
    # Spark::SQL::NumericType
    #
    # Numeric data types.
    #
    class NumericType < AtomicType
    end


    ##
    # Spark::SQL::IntegralType
    #
    # Integral data types.
    #
    class IntegralType < NumericType
    end


    ##
    # Spark::SQL::StringType
    #
    # String data type.
    #
    class StringType < AtomicType
      atomic
    end


    ##
    # Spark::SQL::LongType
    #
    # Long data type, i.e. a signed 64-bit integer.
    #
    # If the values are beyond the range of [-9223372036854775808, 9223372036854775807],
    # please use {DecimalType}.
    #
    class LongType < IntegralType
      atomic
    end

  end
end
