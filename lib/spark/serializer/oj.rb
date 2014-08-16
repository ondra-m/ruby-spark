require_relative "marshal.rb"

module Spark
  module Serializer
    class Oj < Marshal

      def self.serialize(data)
        ::Oj::dump(data)
      end

      def self.deserialize(data)
        ::Oj::load(data)
      end

    end
  end
end

begin
  require "oj"
rescue LoadError
  Spark::Serializer::Oj = Spark::Serializer::Marshal
end
