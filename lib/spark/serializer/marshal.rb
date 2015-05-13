module Spark
  module Serializer
    class Marshal < Base

      def dump(data)
        ::Marshal.dump(data)
      end

      def load(data)
        ::Marshal.load(data)
      end

    end
  end
end

Spark::Serializer.register('marshal', Spark::Serializer::Marshal)
