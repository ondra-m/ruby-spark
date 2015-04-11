module Spark
  module CoreExtension
    module Module

      # Patch method to class unless already exist
      #
      # == Example:
      #
      #   class Hash
      #     def a
      #       1
      #     end
      #   end
      #
      #   module HashExtension
      #     module InstanceMethods
      #       def a_with_spark
      #         2
      #       end
      #
      #       def b_with_spark
      #         1
      #       end
      #     end
      #
      #     def self.included(base)
      #       base.send(:include, InstanceMethods)
      #       base.class_eval do
      #         patch_unless_exist :a, :spark
      #         patch_unless_exist :b, :spark
      #       end
      #     end
      #   end
      #
      #   Hash.include(HashExtension)
      #
      #   Hash.new.a # => 1
      #   Hash.new.b # => 1
      #
      def patch_unless_exist(target, suffix)
        unless method_defined?(target)
          aliased_target, punctuation = target.to_s.sub(/([?!=])$/, ''), $1

          alias_method target, "#{aliased_target}_with_#{suffix}#{punctuation}"
        end
      end

      def path_const_unless_exist(target, suffix)
        unless const_defined?(target)
          const_set(target, const_get("#{target}_WITH_#{suffix}"))
        end
      end

    end
  end
end

Module.__send__(:include, Spark::CoreExtension::Module)
