module Spark
  module Serializer
    class Oj < Marshal

      def name
        'oj'
      end

      def serialize(data)
        ::Oj::dump(data)
      end

      def deserialize(data)
        ::Oj::load(data)
      end

    end
  end
end

begin
  require 'oj'
rescue LoadError
  Spark::Serializer::Oj = Spark::Serializer::Marshal
end
