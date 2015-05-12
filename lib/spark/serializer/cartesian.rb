module Spark
  module Serializer
    class Cartesian < Pair

      def aggregate(item1, item2)
        item1.product(item2)
      end

    end
  end
end

Spark::Serializer.register('cartesian', Spark::Serializer::Cartesian)
