module Spark
  class Serializer
    class Batched < Base

      attr_reader :batch_size

      def initialize(batch_size)
        @batch_size = batch_size.to_i
      end

      def dump(data)
        if batched?
          data.each_slice(batch_size).lazy
        else
          data
        end
      end

      # Serializer.load_from_io should get everytime an Array from this serializer
      # because data can also contains an Array and Serializer does not know
      # if data are correct or just batched.
      #
      # == Example:
      #
      #   # ORIGINAL DATA: [1,2,3]
      #   # BATCH SIZE: 2
      #
      #   # batched? == true
      #   load([1,2]) # => [1,2]
      #   load([3])   # => [3]
      #
      #   # batched? == false
      #   load(1) # => [1]
      #   load(2) # => [2]
      #   load(3) # => [3]
      #
      def load(data, *)
        if batched?
          data
        else
          [data]
        end
      end

      def to_s
        "Batched(#{batch_size})"
      end

      private

        def batched?
          batch_size > 1
        end

    end
  end
end

Spark::Serializer.register('batched', Spark::Serializer::Batched)
