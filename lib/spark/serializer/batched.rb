module Spark
  class Serializer
    class Batched < Simple

      attr_reader :batch_size

      def initialize(serializer, batch_size)
        super(serializer)
        @batch_size = batch_size.to_i

        error('Batch size must be greater than 0') if @batch_size < 1
      end

      # Really batched
      def batched?
        batch_size > 1
      end

      def marshal_dump
        super + [@batch_size]
      end

      def marshal_load(data)
        super
        @batch_size = data.shift
      end

      def to_s
        "Batched(#{batch_size}) -> #{serializer}"
      end


      # === Dump ==============================================================

      def dump_to_io(data, io)
        check_each(data)

        if batched?
          data = data.each_slice(batch_size)
        end

        data.each do |item|
          serialized = @serializer.dump(item)
          io.write_string(serialized)
        end

        io.flush
      end


      # === Load ==============================================================

      def load_from_io(io)
        result = super

        if result && batched?
          result.lazy.flat_map{|x| x}
        else
          result
        end
      end

      def load_from_iterator(iterator)
        result = super
        result.flatten!(1) if batched?
        result
      end

    end
  end
end

Spark::Serializer.register('batched', Spark::Serializer::Batched)
