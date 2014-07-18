require_relative "base.rb"

module Spark
  module Serializer
    class Simple < Base

      # load => Base
      # dump => Base

      private

        def self.load_from_io(io)
          result = []
          while true
            begin
              result << Marshal.load(io.read(io.read(4).unpack("l>")[0]))
            rescue
              break
            end
          end
          result
        end

    end
  end
end
