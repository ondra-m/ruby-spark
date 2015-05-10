module Spark
  class Serializer
    class MessagePack < BasicBase

      def initialize
        require 'msgpack'
      end

      def serialize(data)
        ::MessagePack.dump(data)
      end

      def deserialize(data)
        ::MessagePack.load(data)
      end

    end
  end
end

Spark::Serializer.register('messagepack', 'message_pack', 'msgpack', Spark::Serializer::MessagePack)
