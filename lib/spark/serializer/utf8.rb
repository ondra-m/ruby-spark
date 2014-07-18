module Spark
  module Serializer
    class UTF8
      
      def self.load(source)
        if source.is_a?(IO)
          load_from_io(source)
        elsif source.is_a?(Array)
          load_from_array(source)
        end
      end

      # def self.load_from_itr(iterator)
      #   result = []

      #   while iterator.hasNext
      #     result << Marshal.load(iterator.next.to_a.pack("C*"))
      #   end

      #   result
      # end

      def self.dump(data, io)
        data.map! do|item|
          serialized = Marshal.dump(item)

          [serialized.size].pack("l>") + serialized
        end

        io.write(data.join)
      end

      private

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

        def self.load_from_array(array)
          array.map! do |item|
            Marshal.load(item.pack("C*"))
          end
        end

    end
  end
end
