module Spark
  module Serializer
    class Oj < Base

      def dump(data)
        ::Oj.dump(data)
      end

      def load(data)
        ::Oj.load(data)
      end

    end
  end
end

begin
  # TODO: require only if it is necessary
  require 'oj'

  Spark::Serializer.register('oj', Spark::Serializer::Oj)
rescue LoadError
end
