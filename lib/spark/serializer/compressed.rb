module Spark
  module Serializer
    class Compressed < Base

      def initialize(serializer)
        @serializer = serializer
      end

      def dump(data)
        Zlib::Deflate.deflate(@serializer.dump(data))
      end

      def load(data)
        @serializer.load(Zlib::Inflate.inflate(data))
      end

    end
  end
end

begin
  # TODO: require only if it is necessary
  require 'zlib'

  Spark::Serializer.register('compress', 'compressed', Spark::Serializer::Compressed)
rescue LoadError
end
