module Spark
  class Serializer
    class AutoBatched < Base

      def dump(data)
      end

      def load(data)
      end

    end
  end
end

Spark::Serializer.register('auto_batched', 'autobatched', Spark::Serializer::AutoBatched)
