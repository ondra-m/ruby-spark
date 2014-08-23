require_relative "base.rb"

# Used for file
#
# File is sended as String but worker use serialization
#
module Spark
  module Serializer
    class UTF8 < Base

      UTF8_BATCH_SIZE = 1

      def initialize(_)
        super(UTF8_BATCH_SIZE)
      end

      def load_from_io(io)
        result = []
        while true
          begin
            result << io.read(unpack_int(io.read(4)))
          rescue
            break
          end
        end
        result
      end

    end
  end
end
