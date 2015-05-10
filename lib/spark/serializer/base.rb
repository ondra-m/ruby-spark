module Spark
  class Serializer
    class Base

      def to_s
        self.class.name.split('::').last
      end

    end
  end
end

