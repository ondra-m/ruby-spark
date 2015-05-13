module Spark
  module CoreExtension
    module IO
      module ClassMethods
      end

      module InstanceMethods

        # Reading

        def read_int
          unpack_int(read(4))
        end

        def read_int_or_eof
          bytes = read(4)
          return Spark::Constant::DATA_EOF if bytes.nil?
          unpack_int(bytes)
        end

        def read_long
          unpack_long(read(8))
        end

        def read_string
          read(read_int)
        end

        def read_data
          Marshal.load(read_string)
        end


        # Writing

        def write_int(data)
          write(pack_int(data))
        end

        def write_long(data)
          write(pack_long(data))
        end

        # Size and data can have different encoding
        # Marshal: both ASCII
        # Oj: ASCII and UTF-8
        def write_string(data)
          write_int(data.bytesize)
          write(data)
        end

        def write_data(data)
          write_string(Marshal.dump(data))
        end
      end

      def self.included(base)
        base.extend(ClassMethods)
        base.send(:include, Spark::Helper::Serialize)
        base.send(:include, InstanceMethods)
      end
    end
  end
end

IO.__send__(:include, Spark::CoreExtension::IO)
StringIO.__send__(:include, Spark::CoreExtension::IO)
