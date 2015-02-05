module Spark
  module Serializer
    class Cartesian < Base

      attr_reader :first, :second

      def set(first, second)
        @first  = first
        @second = second
        self
      end

      # Little hack
      # Data does not have to be batched but items are added by <<
      def batched?
        true
      end

      def load_next_from_io(io, lenght)
        item1 = io.read(lenght)
        item2 = io.read_string
        deserialize(item1, item2)
      end

      def deserialize(item1, item2)
        deserialized_item1 = @first.deserialize(item1)
        deserialized_item2 = @second.deserialize(item2)

        deserialized_item1 = [deserialized_item1] unless @first.batched?
        deserialized_item2 = [deserialized_item2] unless @second.batched?

        deserialized_item1.product(deserialized_item2)
      end

    end
  end
end
