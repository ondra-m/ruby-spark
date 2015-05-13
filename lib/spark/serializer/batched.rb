module Spark
  module Serializer
    class Batched < Base

      attr_writer :serializer

      def initialize(serializer, batch_size=nil)
        batch_size ||= Spark::Serializer::DEFAULT_BATCH_SIZE

        @serializer = serializer
        @batch_size = batch_size.to_i

        error('Batch size must be greater than 0') if @batch_size < 1
      end

      # Really batched
      def batched?
        @batch_size > 1
      end

      def unbatch!
        @batch_size = 1
      end

      def load(data)
        @serializer.load(data)
      end

      def dump(data)
        @serializer.dump(data)
      end

      def name
        "Batched(#{@batch_size})"
      end

      def to_s
        "#{name} -> #{@serializer}"
      end


      # === Dump ==============================================================

      def dump_to_io(data, io)
        check_each(data)

        if batched?
          data = data.each_slice(@batch_size)
        end

        data.each do |item|
          serialized = dump(item)
          io.write_string(serialized)
        end

        io.flush
      end


      # === Load ==============================================================

      def load_from_io(io)
        return to_enum(__callee__, io) unless block_given?

        loop do
          size = io.read_int_or_eof
          break if size == Spark::Constant::DATA_EOF

          data = io.read(size)
          data = load(data)

          if batched?
            data.each{|item| yield item }
          else
            yield data
          end
        end
      end

    end
  end
end

Spark::Serializer.register('batched', Spark::Serializer::Batched)
