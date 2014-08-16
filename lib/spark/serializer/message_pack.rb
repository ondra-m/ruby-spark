require_relative "base.rb"

require "msgpack"

module Spark
  module Serializer
    class MessagePack < Base

      def self.serialize(data)
        ::MessagePack::dump(data)
      end

      def self.deserialize(data)
        ::MessagePack::load(data)
      end

    end
  end
end

