require "java"

module Spark
  module JavaBridge
    class JRuby < Base

      def import
        jars.each {|jar| require jar}

        java_objects.each do |key, value|
          value = "Java::#{value}"
          Object.const_set(key, eval(value)) rescue nil
        end
      end

      def java_object?(object)
        # object.respond_to?(:_classname)
        object.is_a?(JavaProxy)
      end

    end
  end
end
