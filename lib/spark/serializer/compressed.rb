module Spark
  class Serializer
    class Compressed < Base

      def initialize
        require 'zlib'
      end

      def dump(data)
        data.map do |item|
          Zlib::Deflate.deflate(item)
        end
      end

      def load(data, *)
        Zlib::Inflate.inflate(data)
      end

    end
  end
end

Spark::Serializer.register('compress', 'compressed', Spark::Serializer::Compressed)
