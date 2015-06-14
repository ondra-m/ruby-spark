module Spark
  module Serializer
    ##
    # AutoBatched serializator
    #
    # Batch size is computed automatically. Simillar to Python's AutoBatchedSerializer.
    #
    class AutoBatched < Batched

      MAX_RATIO = 10

      def initialize(serializer, best_size=65536)
        @serializer = serializer
        @best_size = best_size.to_i

        error('Batch size must be greater than 1') if @best_size < 2
      end

      def batched?
        true
      end

      def unbatch!
      end

      def name
        "AutoBatched(#{@best_size})"
      end

      def dump_to_io(data, io)
        check_each(data)

        # Only Array have .slice
        data = data.to_a

        index = 0
        batch = 2
        max = @best_size * MAX_RATIO

        loop do
          chunk = data.slice(index, batch)
          if chunk.nil? || chunk.empty?
            break
          end

          serialized = @serializer.dump(chunk)
          io.write_string(serialized)

          index += batch

          size = serialized.bytesize
          if size < @best_size
            batch *= 2
          elsif size > max && batch > 1
            batch /= 2
          end
        end

        io.flush
      end

    end
  end
end

Spark::Serializer.register('auto_batched', 'autobatched', Spark::Serializer::AutoBatched)
