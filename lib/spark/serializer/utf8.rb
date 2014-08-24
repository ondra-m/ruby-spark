require_relative "base.rb"

# Used for file
#
# File is sended as String but worker use serialization
#
module Spark
  module Serializer
    class UTF8 < Base

      def set(*)
        unbatch!
        self
      end

      def batched?
        false
      end

      def load_one_from_io(io)
        io.read(unpack_int(io.read(4))).force_encoding(Encoding::UTF_8)
      end

    end
  end
end
