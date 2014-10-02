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

    end
  end
end
