module Spark
  module Serializer
    class Base

      extend Spark::Serializer::Helper

      def self.load(source)
        if source.is_a?(IO)
          load_from_io(source)
        elsif source.is_a?(Array)
          load_from_array(source)
        elsif try(source, :iterator)
          # mri: respond_to?(:iterator) => false
          # jruby: respond_to?(:iterator) => true
          load_from_iterator(source.iterator)
        end
      end

      def self.dump(data, io)
        data = [data] unless data.is_a?(Array)

        data.map! do |item|
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
            if item.is_a?(String)
              # do nothing
            else
              item = pack_unsigned_chars(item)
            end
            Marshal.load(item)
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
            result << Marshal.load(item)
          end
          result
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
