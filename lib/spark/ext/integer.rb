module Spark
  module CoreExtension
    module Integer
      module ClassMethods
      end

      module InstanceMethods
      end

      def self.included(base)
        base.extend(ClassMethods)
        base.send(:include, InstanceMethods)
        base.class_eval do
          const_set :MAX_WITH_SPARK, 1 << (1.size * 8 - 2) - 1
          const_set :MIN_WITH_SPARK, -const_get(:MAX_WITH_SPARK) - 1

          path_const_unless_exist :MAX, :SPARK
          path_const_unless_exist :MIN, :SPARK
        end
      end
    end
  end
end

Integer.__send__(:include, Spark::CoreExtension::Integer)
