require_relative "base.rb"

module Spark
  module Serializer
    class Pairwise < Base
      
      def self.dump(data, io)
        data = [data] unless data.is_a?(Array)

        data.map! do |key, item|
          key = [key].pack("q>")
          serialized = Marshal.dump(item)

          [key.size].pack("l>") + key + [serialized.size].pack("l>") + serialized
        end

        io.write(data.join)
      end

    end
  end
end
