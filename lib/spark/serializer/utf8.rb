module Spark
  module Serializer
    class UTF8
      
      def self.load(bytes_array)
        result = []

        bytes_array.each do |bytes|
          result << Marshal.load(bytes.pack("C*"))
        end

        result
      end

    end
  end
end
