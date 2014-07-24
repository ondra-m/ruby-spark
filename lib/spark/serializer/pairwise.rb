require_relative "base.rb"

module Spark
  module Serializer
    class Pairwise < Base
      
      def self.dump(data, io)
        data = [data] unless data.is_a?(Array)

        data.map! do |key, item|
          key = pack_long(key)
          serialized = Marshal.dump(item)

          pack_int(key.size) + key + pack_int(serialized.size) + serialized
        end

        io.write(data.join)
      end

    end
  end
end
