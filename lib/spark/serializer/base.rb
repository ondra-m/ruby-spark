module Spark
  module Serializer
    # @abstract Parent for all serializers
    class Base

      def load_from_io(io)
        return to_enum(__callee__, io) unless block_given?

        loop do
          size = io.read_int_or_eof
          break if size == Spark::Constant::DATA_EOF

          yield load(io.read(size))
        end
      end

      def load_from_file(file, *args)
        return to_enum(__callee__, file, *args) unless block_given?

        load_from_io(file, *args).each do |item|
          yield item
        end

        file.close
        file.unlink
      end

      def ==(other)
        self.to_s == other.to_s
      end

      def batched?
        false
      end

      def unbatch!
      end

      def check_each(data)
        unless data.respond_to?(:each)
          error('Data must be iterable.')
        end
      end

      def error(message)
        raise Spark::SerializeError, message
      end

      def name
        self.class.name.split('::').last
      end

      def to_s
        name
      end

      def inspect
        %{#<Spark::Serializer:0x#{object_id}  "#{self}">}
      end

    end
  end
end
