module Spark
  class Serializer
    class Batched < Base

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

      def load_from_io(io)
        # Lazy deserialization
        return to_enum(__callee__, io) unless block_given?

        loop do
          size = io.read_int_or_eof
          break if size == Spark::Constant::DATA_EOF

          # Load one item
          data = io.read(size)
          data = @serializer.load(data)

          if batched?
            data.each {|item| yield item }
          else
            yield data
          end
        end
      end

      # Load from Java iterator by calling hasNext and next
      def load_from_iterator(iterator)
        result = []
        while iterator.hasNext
          item = iterator.next

          # mri: data are String
          # jruby: data are bytes Array

          if item.is_a?(String)
            # Serialized data
            result << @serializer.load(item)
          else
            case item.getClass.getSimpleName
            when 'byte[]'
              result << @serializer.load(pack_unsigned_chars(item.to_a))
            # when 'Tuple2'
            #   result << deserialize(item._1, item._2)
            else
              raise Spark::SerializeError, "Cannot deserialize #{item.getClass.getSimpleName} class."
            end
          end
        end

        result.flatten!(1) if batched?
        result
      end

    end
  end
end

Spark::Serializer.register('batched', Spark::Serializer::Batched)
