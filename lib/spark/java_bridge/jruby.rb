require 'java'

module Spark
  module JavaBridge
    class JRuby < Base

      def initialize(*args)
        super
        jars.each {|jar| require jar}
      end

      def import(name, klass)
        klass = "Java::#{klass}"
        Object.const_set(name, eval(klass))
      rescue NameError
        raise_missing_class(klass)
      end

      def java_object?(object)
        object.is_a?(JavaProxy)
      end

    end
  end
end
