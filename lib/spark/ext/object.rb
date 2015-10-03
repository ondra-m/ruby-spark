module Spark
  module CoreExtension
    module Object
      module ClassMethods
      end

      module InstanceMethods
        def deep_copy_with_spark
          Marshal.load(Marshal.dump(self))
        end

        def silence_warnings
          old_verbose, $VERBOSE = $VERBOSE, nil
          yield
        ensure
          $VERBOSE = old_verbose
        end

        def cattr_reader_with_spark(*syms)
          syms.each do |sym|
            raise NameError.new("Invalid attribute name: #{sym}") unless sym =~ /^[_A-Za-z]\w*$/

            class_eval(<<-EOS, __FILE__, __LINE__ + 1)
              @@#{sym} = nil unless defined? @@#{sym}
              def self.#{sym}
                @@#{sym}
              end
            EOS

            class_eval(<<-EOS, __FILE__, __LINE__ + 1)
              def #{sym}
                @@#{sym}
              end
            EOS
          end
        end

        def cattr_writer_with_spark(*syms)
          syms.each do |sym|
            raise NameError.new("Invalid attribute name: #{sym}") unless sym =~ /^[_A-Za-z]\w*$/

            class_eval(<<-EOS, __FILE__, __LINE__ + 1)
              @@#{sym} = nil unless defined? @@#{sym}
              def self.#{sym}=(obj)
                @@#{sym} = obj
              end
            EOS

            class_eval(<<-EOS, __FILE__, __LINE__ + 1)
              def #{sym}=(obj)
                @@#{sym} = obj
              end
            EOS
          end
        end

        def cattr_accessor_with_spark(*syms)
          cattr_reader_with_spark(*syms)
          cattr_writer_with_spark(*syms)
        end
      end

      def self.included(base)
        base.extend(ClassMethods)
        base.send(:include, InstanceMethods)
        base.class_eval do
          patch_unless_exist :deep_copy, :spark
          patch_unless_exist :silence_warnings, :spark
          patch_unless_exist :cattr_accessor, :spark
        end
      end
    end
  end
end

Object.__send__(:include, Spark::CoreExtension::Object)
