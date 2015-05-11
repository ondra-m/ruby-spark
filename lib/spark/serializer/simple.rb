module Spark
  class Serializer
    class Simple

      attr_reader :serializer

      def initialize(serializer)
        @serializer = serializer
        after_initialize
      end

      def ==(other)
        self.to_s == other.to_s
      end

      def check_each(data)
        unless data.respond_to?(:each)
          error('Data must be iterable.')
        end
      end

      def to_s
        "#{self.class.name.split('::').last} -> #{serializer}"
      end

      def inspect
        %{#<Spark::Serializer:0x#{object_id}  "#{self}">}
      end


      # === Marshaling ========================================================

      def marshal_dump
        [@serializer]
      end

      def marshal_load(data)
        before_marshal_load
        @serializer = data.shift
      end


      # === Callbacks =========================================================

      def after_initialize
      end

      def before_marshal_load
      end


      # === Load ==============================================================

      def load_from_io(io)
        # Lazy deserialization
        return to_enum(__callee__, io) unless block_given?

        loop do
          size = io.read_int_or_eof
          break if size == Spark::Constant::DATA_EOF

          # Load one item
          data = io.read(size)
          data = @serializer.load(data)

          yield data
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
        result
      end

      private

        def error(message)
          raise Spark::SerializeError, message
        end

    end
  end
end

Spark::Serializer.register('base', 'basic', 'simple', Spark::Serializer::Simple)
