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

    end
  end
end
