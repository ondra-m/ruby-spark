module Spark
  class Serializer
    class Base

      attr_reader :serializer

      def initialize(serializer)
        @serializer = serializer
        after_initialize
      end

      def check_each(data)
        unless data.respond_to?(:each)
          error('Data must be iterable.')
        end
      end

      def to_s
        "#{self.class.name.split('::').last} -> #{serializer}"
      end

      def inspect
        %{#<Spark::Serializer:0x#{object_id}  "#{to_s}">}
      end


      # === Marshaling ========================================================

      def marshal_dump
        [@serializer]
      end

      def marshal_load(data)
        before_marshal_load
        @serializer = data.shift
      end

      def _marshal_dump
      end

      def _marshal_load(*)
      end


      # === Callbacks =========================================================

      def after_initialize
      end

      def before_marshal_load
      end

      private

        def error(message)
          raise Spark::SerializeError, message
        end

    end
  end
end

