module Spark
  module Serializer
    class Marshal < Base

      def name
        "marshal"
      end

      def serialize(data)
        ::Marshal::dump(data)
      end

      def deserialize(data)
        ::Marshal::load(data)
      end

    end
  end
end
