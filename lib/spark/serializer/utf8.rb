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

      def load_next_from_io(io, lenght)
        io.read(lenght).force_encoding(Encoding::UTF_8)
      end

    end
  end
end
