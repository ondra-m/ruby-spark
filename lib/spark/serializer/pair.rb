require_relative "base.rb"

module Spark
  module Serializer
    class Pair < Base

      attr_reader :first, :second

      def set(first, second)
        unbatch!
        @first  = first
        @second = second
        self
      end

      def batched?
        false
      end

      def load_one_from_io(io)
        key_value = []
        key_value << @first.load_one_from_io(io)
        key_value << @second.load_one_from_io(io)
        key_value
      end

    end
  end
end
