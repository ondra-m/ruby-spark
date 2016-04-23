module Spark
  module SQL
    ##
    # Spark::SQL::Row
    #
    class Row
      attr_reader :data

      def self.from_java(object, with_schema=true)
        if with_schema
          fields = object.schema.fieldNames
        else
          # Create virtual schema (t0, t1, t2, ...)
          raise Spark::NotImplemented, 'Row must have a schema'
        end

        if object.anyNull
          data = {}
          object.size.times do |i|
            if object.isNullAt(i)
              value = nil
            else
              value = Spark.jb.to_ruby(object.get(i))
            end

            data[ fields[i] ] = value
          end
        else
          data = fields.zip(Spark.jb.to_ruby(object.values))
        end

        Row.new(data)
      end

      def initialize(data={})
        @data = data.to_h
      end

      def [](item)
        @data[item]
      end

      def to_h
        @data
      end

      def inspect
        formated = data.map do |key, value|
          "#{key}: \"#{value}\""
        end

        %{#<Row(#{formated.join(', ')})>}
      end

    end
  end
end
