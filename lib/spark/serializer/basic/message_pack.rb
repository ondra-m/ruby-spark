module Spark
  class Serializer
    class MessagePack < BasicBase

      def after_initialize
        require 'msgpack'
      end

      def before_marshal_load
        after_initialize
      end

      def dump(data)
        ::MessagePack.dump(data)
      end

      def load(data)
        ::MessagePack.load(data)
      end

    end
  end
end

Spark::Serializer.register('messagepack', 'message_pack', 'msgpack', 'msg_pack', Spark::Serializer::MessagePack)
