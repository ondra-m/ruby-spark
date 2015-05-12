module Spark
  module Serializer
    class MessagePack < Base

      def dump(data)
        ::MessagePack.dump(data)
      end

      def load(data)
        ::MessagePack.load(data)
      end

    end
  end
end

begin
  # TODO: require only if it is necessary
  require 'msgpack'

  Spark::Serializer.register('messagepack', 'message_pack', 'msgpack', 'msg_pack', Spark::Serializer::MessagePack)
rescue LoadError
end
