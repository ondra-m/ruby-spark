module Spark
  module CoreExtension
    module IPSocket
      module ClassMethods
      end

      module InstanceMethods
        def port
          addr[1]
        end

        def hostname
          addr(true)[2]
        end

        def numeric_address
          addr[3]
        end
      end

      def self.included(base)
        base.extend(ClassMethods)
        base.send(:include, InstanceMethods)
      end
    end
  end
end

IPSocket.__send__(:include, Spark::CoreExtension::IPSocket)
