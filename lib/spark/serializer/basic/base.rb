module Spark
  class Serializer
    class BasicBase

      def initialize
        after_initialize
      end

      def ==(other)
        self.to_s == other.to_s
      end

      def marshal_dump
      end

      def marshal_load(*)
        before_marshal_load
      end

      def after_initialize
      end

      def before_marshal_load
      end

      def to_s
        self.class.name.split('::').last
      end

    end
  end
end
