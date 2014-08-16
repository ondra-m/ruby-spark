module Spark
  module Serializer
    class Base

      extend Spark::Serializer::Helper

      # ===========================================================================
      # Load

      # mri: respond_to?(:iterator) => false
      # jruby: respond_to?(:iterator) => true
      def self.load(source)
        if source.is_a?(IO)
          load_from_io(source)
        elsif source.is_a?(Array)
          load_from_array(source)
        elsif try(source, :iterator)
          load_from_iterator(source.iterator)
        end
      end

      def self.load_from_io(io)
        result = []
        while true
          begin
            result << deserialize(io.read(unpack_int(io.read(4))))
          rescue
            break
          end
        end
        result
      end

      def self.load_from_array(array)
        array.map! do |item|
          if item.is_a?(String)
            # do nothing
          else
            item = pack_unsigned_chars(item)
          end
          deserialize(item)
        end
      end

      # Java iterator
      def self.load_from_iterator(iterator)
        result = []
        while iterator.hasNext
          item = iterator.next
          if item.is_a?(String)
            # do nothing
          else
            item = pack_unsigned_chars(item.to_a)
          end
          result << deserialize(item)
        end
        result
      end

      # ===========================================================================
      # Dump

      def self.dump(data, io)
        data = [data] unless data.is_a?(Array)

        data.map! do |item|
          serialized = serialize(item)

          pack_int(serialized.size) + serialized
        end

        io.write(data.join)
      end

      def self.dump_to_java(data)
        data.map! do |item|
          serialize(item).to_java_bytes
        end
      end

      # Rescue cannot be defined
      #
      # mri => RuntimeError
      # jruby => NoMethodError
      #
      def self.try(object, method)
        begin
          object.send(method)
          return true
        rescue
          return false
        end
      end

    end
  end
end
