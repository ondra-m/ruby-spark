module Spark
  module Helper
    module Serialize

      # http://www.ruby-doc.org/core-2.1.1/String.html#method-i-unpack

      def pack_unsigned_chars(data)
        pack(data, :unsigned_chars)
      end

      def pack_int(data)
        pack([data], :int)
      end

      def pack_long(data)
        pack([data], :long)
      end

      def unpack_int(data)
        unpack(data, :int)
      end

      def unpack_long(data)
        unpack(data, :long)
      end

      def unpack_chars(data)
        unpack(data, :chars)
      end

      def pack(data, type)
        case type.to_sym
        when :unsigned_chars
          data.pack("C*")
        when :int
          data.pack("l>")
        when :long
          data.pack("q>")
        else
          raise "Unknow type: #{type}."
        end
      end

      def unpack(data, type)
        case type.to_sym
        when :chars
          data.unpack("c*")
        when :int
          data.unpack("l>")[0]
        when :long
          data.unpack("q>")[0]
        else
          raise "Unknow type: #{type}."
        end
      end

    end
  end
end
