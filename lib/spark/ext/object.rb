module Spark
  module CoreExtension
    module Object
      module ClassMethods
      end
      
      module InstanceMethods
        def deep_copy_with_spark
          Marshal.load(Marshal.dump(self))
        end
      end
      
      def self.included(base)
        base.extend(ClassMethods)
        base.send(:include, InstanceMethods)
        base.class_eval do
          patch_unless_exist :deep_copy, :spark
        end
      end
    end
  end
end

Object.include(Spark::CoreExtension::Object)
