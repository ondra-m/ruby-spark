module Spark
  module CoreExtension
    module Hash
      module ClassMethods
      end

      module InstanceMethods
        # Destructively convert all keys to strings.
        def stringify_keys_with_spark!
          transform_keys!{ |key| key.to_s }
        end

        # Destructively convert all keys to symbols, as long as they respond
        def symbolize_keys_with_spark!
          transform_keys!{ |key| key.to_sym rescue key }
        end

        # Destructively convert all keys using the block operations.
        # Same as transform_keys but modifies +self+.
        def transform_keys_with_spark!
          keys.each do |key|
            self[yield(key)] = delete(key)
          end
          self
        end
      end

      def self.included(base)
        base.extend(ClassMethods)
        base.send(:include, InstanceMethods)
        base.class_eval do
          patch_unless_exist :stringify_keys!, :spark
          patch_unless_exist :symbolize_keys!, :spark
          patch_unless_exist :transform_keys!, :spark
        end
      end
    end
  end
end

Hash.__send__(:include, Spark::CoreExtension::Hash)
