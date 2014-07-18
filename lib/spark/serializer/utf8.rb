require_relative "base.rb"

#
# Used for file
#
# File is sended as String but worker use serialization
#
module Spark
  module Serializer
    class UTF8 < Base

      # load => Base
      # dump => Base

      private

        def self.load_from_io(io)
          result = []
          while true
            begin
              result << io.read(io.read(4).unpack("l>")[0])
            rescue
              break
            end
          end
          result
        end

    end
  end
end
