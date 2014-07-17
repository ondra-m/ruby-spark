module Spark
  module Serializer
    class UTF8
      
      def self.load(bytes_array)
        bytes_array.map! do |bytes|
          Marshal.load(bytes.pack("C*"))
        end
      end

      def self.load_from_itr(iterator)
        result = []

        while iterator.hasNext
          result << Marshal.load(iterator.next.to_a.pack("C*"))
        end

        result
      end

      def self.load_from_io(stream)
        result = []
        while true
          begin
            result << stream.read(stream.read(4).unpack("l>")[0])
          rescue
            break
          end
        end
        result
      end

      def self.dump_for_io(data)
        data.map! do|item|
          serialized = Marshal.dump(item)

          [serialized.size].pack("l>") + serialized
        end
      end

    end
  end
end
