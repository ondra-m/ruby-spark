module Spark
  module CoreExtension
    module String
      module ClassMethods
      end

      module InstanceMethods
        def camelize_with_spark
          self.gsub(/\/(.?)/) { "::#{$1.upcase}" }.gsub(/(?:^|_)(.)/) { $1.upcase }
        end
      end

      def self.included(base)
        base.extend(ClassMethods)
        base.send(:include, InstanceMethods)
        base.class_eval do
          patch_unless_exist :camelize, :spark
        end
      end
    end
  end
end

String.__send__(:include, Spark::CoreExtension::String)
