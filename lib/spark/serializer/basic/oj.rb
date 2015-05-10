module Spark
  class Serializer
    class Oj < BasicBase

      def after_initialize
        require 'oj'
      end

      def before_marshal_load
        after_initialize
      end

      def dump(data)
        ::Oj.dump(data)
      end

      def load(data)
        ::Oj.load(data)
      end

    end
  end
end

Spark::Serializer.register('oj', Spark::Serializer::Oj)
