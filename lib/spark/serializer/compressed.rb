module Spark
  class Serializer
    class Compressed < Simple

      def after_initialize
        require 'zlib'
      end

      def before_marshal_load
        after_initialize
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

Spark::Serializer.register('compress', 'compressed', Spark::Serializer::Compressed)
