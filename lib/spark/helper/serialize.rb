module Spark
  module Helper
    module Serialize

      DIRECTIVE_INTEGER_BIG_ENDIAN = 'l>'
      DIRECTIVE_INTEGERS_BIG_ENDIAN = 'l>*'
      DIRECTIVE_LONG_BIG_ENDIAN = 'q>'
      DIRECTIVE_LONGS_BIG_ENDIAN = 'q>*'
      DIRECTIVE_DOUBLE_BIG_ENDIAN = 'G'
      DIRECTIVE_DOUBLES_BIG_ENDIAN = 'G*'
      DIRECTIVE_UNSIGNED_CHARS = 'C*'
      DIRECTIVE_CHARS = 'c*'

      # Packing

      def pack_int(data)
        [data].pack(DIRECTIVE_INTEGER_BIG_ENDIAN)
      end

      def pack_long(data)
        [data].pack(DIRECTIVE_LONG_BIG_ENDIAN)
      end

      def pack_double(data)
        [data].pack(DIRECTIVE_DOUBLE_BIG_ENDIAN)
      end

      def pack_unsigned_chars(data)
        data.pack(DIRECTIVE_UNSIGNED_CHARS)
      end

      def pack_ints(data)
        __check_array(data)
        data.pack(DIRECTIVE_INTEGERS_BIG_ENDIAN)
      end

      def pack_longs(data)
        __check_array(data)
        data.pack(DIRECTIVE_LONGS_BIG_ENDIAN)
      end

      def pack_doubles(data)
        __check_array(data)
        data.pack(DIRECTIVE_DOUBLES_BIG_ENDIAN)
      end

      # Unpacking

      def unpack_int(data)
        data.unpack(DIRECTIVE_INTEGER_BIG_ENDIAN)[0]
      end

      def unpack_long(data)
        data.unpack(DIRECTIVE_LONG_BIG_ENDIAN)[0]
      end

      def unpack_chars(data)
        data.unpack(DIRECTIVE_CHARS)
      end

      private

        def __check_array(data)
          unless data.is_a?(Array)
            raise ArgumentError, 'Data must be an Array.'
          end
        end

    end
  end
end
