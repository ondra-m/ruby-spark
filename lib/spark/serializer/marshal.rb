require_relative "base.rb"

module Spark
  module Serializer
    class Marshal < Base

      def serialize(data)
        ::Marshal::dump(data)
      end

      def deserialize(data)
        ::Marshal::load(data)
      end

    end
  end
end
