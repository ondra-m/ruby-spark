module Spark
  class Serializer
    class Oj < BasicBase

      def initialize
        require 'oj'
      end

      def serialize(data)
        ::Oj.dump(data)
      end

      def deserialize(data)
        ::Oj.load(data)
      end

    end
  end
end

Spark::Serializer.register('oj', Spark::Serializer::Oj)
