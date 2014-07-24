module Spark
  module Serializer
    class Base

      extend Spark::Serializer::Helper

      def self.load(source)
        if source.is_a?(IO)
          load_from_io(source)
        elsif source.is_a?(Array)
          load_from_array(source)
        elsif source.respond_to?(:iterator)
          load_from_iterator(source)
        end
      end

      def self.dump(data, io)
        data = [data] unless data.is_a?(Array)

        data.map! do|item|
          serialized = Marshal.dump(item)

          pack_int(serialized.size) + serialized
        end

        io.write(data.join)
      end

      private

        def self.load_from_io(io)
          raise Spark::NotImplemented, "DO NOT USE Base.load_from_io"
        end

        def self.load_from_array(array)
          array.map! do |item|
            Marshal.load(pack_unsigned_chars(item))
          end
        end

        # Java iterator
        def self.load_from_iterator(iterator)
          result = []
          while iterator.hasNext
            result << Marshal.load(pack_unsigned_chars(iterator.next.to_a))
          end
          result
        end


    end
  end
end
