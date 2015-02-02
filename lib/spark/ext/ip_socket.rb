module Spark
  module CoreExtension
    module IPSocket

      INTEGER_BIG_ENDIAN = 'l>'

      def port
        addr[1]
      end

      def hostname
        addr(true)[2]
      end

      def numeric_address
        addr[3]
      end

      def write_int(data)
        write([data].pack(INTEGER_BIG_ENDIAN))
      end

      def write_string(data)
        write_int(data.size)
        write(data)
      end

      def write_data(data)
        write_string(Marshal.dump(data))
      end

      def read_int
        read(4).unpack(INTEGER_BIG_ENDIAN)[0]
      end

      def read_string
        read(read_int)
      end

      def read_data
        Marshal.load(read_string)
      end

    end
  end
end

IPSocket.include(Spark::CoreExtension::IPSocket)
