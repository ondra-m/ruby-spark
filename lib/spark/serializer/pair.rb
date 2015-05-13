module Spark
  module Serializer
    class Pair < Base

      def initialize(serializer1, serializer2)
        @serializer1 = serializer1
        @serializer2 = serializer2
      end

      def to_s
        "#{name}(#{@serializer1}, #{@serializer2})"
      end

      def aggregate(item1, item2)
        item1.zip(item2)
      end

      def load_from_io(io)
        return to_enum(__callee__, io) unless block_given?

        loop do
          size = io.read_int_or_eof
          break if size == Spark::Constant::DATA_EOF

          item1 = @serializer1.load(io.read(size))
          item2 = @serializer2.load(io.read_string)

          item1 = [item1] unless @serializer1.batched?
          item2 = [item2] unless @serializer2.batched?

          aggregate(item1, item2).each do |item|
            yield item
          end
        end
      end

    end
  end
end

Spark::Serializer.register('pair', Spark::Serializer::Pair)
