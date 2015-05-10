module Spark
  class Serializer
    class Marshal < BasicBase

      def serialize(data)
        ::Marshal.dump(data)
      end

      def deserialize(data)
        ::Marshal.load(data)
      end

    end
  end
end

Spark::Serializer.register('marshal', Spark::Serializer::Marshal)
