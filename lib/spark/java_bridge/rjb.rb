if !ENV.has_key?('JAVA_HOME')
  raise Spark::ConfigurationError, 'Environment variable JAVA_HOME is not set'
end

require 'rjb'

module Spark
  module JavaBridge
    class RJB < Base

      def jars
        separator = windows? ? ';' : ':'
        super.join(separator)
      end

      def import
        Rjb::load(jars)
        Rjb::primitive_conversion = true

        java_objects.each do |key, value|
          # Avoid 'already initialized constant'
          Object.const_set(key, silence_warnings { Rjb::import(value) })
        end
      end

      def silence_warnings
        old_verbose, $VERBOSE = $VERBOSE, nil
        yield
      ensure
        $VERBOSE = old_verbose
      end

      def java_object?(object)
        # object.respond_to?(:_classname)
        object.is_a?(Rjb::Rjb_JavaProxy)
      end

    end
  end
end
