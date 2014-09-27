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

      def load_next_from_io(io, lenght)
        key_value = []
        key_value << @first.load_next_from_io(io, lenght)
        key_value << @second.load_next_from_io(io, read_int(io))
        key_value
      end

    end
  end
end
