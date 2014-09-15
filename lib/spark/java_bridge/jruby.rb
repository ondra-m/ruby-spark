require "java"

module Spark
  module JavaBridge
    class JRuby < Base

      def import
        jars.each {|jar| require jar}

        java_objects.each do |key, value|
          Object.const_set(key, eval(value))
        end
      end

    end
  end
end
