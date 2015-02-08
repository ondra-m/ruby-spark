module Spark
  module CoreExtension
    module IO
      module ClassMethods
      end

      module InstanceMethods
        INTEGER_BIG_ENDIAN = 'l>'
        LONG_BIG_ENDIAN = 'q>'

        # Reading

        def read_int
          read(4).unpack(INTEGER_BIG_ENDIAN)[0]
        end

        def read_long
          read(8).unpack(LONG_BIG_ENDIAN)[0]
        end

        def read_string
          read(read_int)
        end

        def read_data
          Marshal.load(read_string)
        end


        # Writing

        def write_int(data)
          write([data].pack(INTEGER_BIG_ENDIAN))
        end

        def write_long(data)
          write([data].pack(LONG_BIG_ENDIAN))
        end

        def write_string(data)
          write_int(data.size)
          write(data)
        end

        def write_data(data)
          write_string(Marshal.dump(data))
        end
      end

      def self.included(base)
        base.extend(ClassMethods)
        base.send(:include, InstanceMethods)
      end
    end
  end
end

IO.include(Spark::CoreExtension::IO)
