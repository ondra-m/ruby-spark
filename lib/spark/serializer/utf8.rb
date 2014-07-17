module Spark
  module Serializer
    class UTF8
      
      def self.load(bytes_array)
        # result = []

        # bytes_array.each do |bytes|
        #   result << Marshal.load(bytes.pack("C*"))
        # end

        # result

        bytes_array.map!{ |bytes|
          Marshal.load(bytes.pack("C*"))
        }
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
