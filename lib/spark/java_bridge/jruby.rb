require "java"

module Spark
  module JavaBridge
    class JRuby < Base

      def import
        jars.each {|jar| require jar}

        java_objects.each do |key, value|
          if value.split(".").first == "scala"
            value = "Java.#{value}"
          end
          Object.const_set(key, eval(value)) rescue nil
        end
      end

    end
  end
end
