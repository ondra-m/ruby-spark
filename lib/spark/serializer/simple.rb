module Spark
  module Serializer
    class Simple
      
      def self.dump(data, stream)
        data.map! do |item|
          serialized = Marshal.dump(item)

          [serialized.size].pack("l>") + serialized
        end

        stream.write(data.join)
      end

    end
  end
end
