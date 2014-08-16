require_relative "base.rb"

module Spark
  module Serializer
    class Marshal < Base

      def self.serialize(data)
        ::Marshal::dump(data)
      end

      def self.deserialize(data)
        ::Marshal::load(data)
      end

    end
  end
end
