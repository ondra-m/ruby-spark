if !ENV.has_key?('JAVA_HOME')
  raise Spark::ConfigurationError, 'Environment variable JAVA_HOME is not set'
end

require 'rjb'

module Spark
  module JavaBridge
    class RJB < Base

      def initialize(*args)
        super
        Rjb.load(jars)
        Rjb.primitive_conversion = true
      end

      def import(name, klass)
        Object.const_set(name, silence_warnings { Rjb.import(klass) })
      rescue NoClassDefFoundError
        raise_missing_class(klass)
      end

      def java_object?(object)
        object.is_a?(Rjb::Rjb_JavaProxy)
      end

      private

        def jars
          separator = windows? ? ';' : ':'
          super.join(separator)
        end

    end
  end
end
