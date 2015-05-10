module Spark
  class Serializer
    class BasicBase < Base

      def dump(data)
        data.map do |item|
          serialize(item)
        end
      end

      def load(data, *)
        deserialize(data)
      end

    end
  end
end
