module Spark
  module Helper
    module System

      def self.included(base)
        base.send :extend,  Methods
        base.send :include, Methods
      end
     
      module Methods
        def windows?
          RbConfig::CONFIG['host_os'] =~ /mswin|mingw/
        end

        def mri?
          RbConfig::CONFIG['ruby_install_name'] == 'ruby'
        end

        def jruby?
          RbConfig::CONFIG['ruby_install_name'] == 'jruby'
        end

        def pry?
          !!Thread.current[:__pry__]
        end

        # Memory usage in kb
        def memory_usage
          if jruby?
            runtime = java.lang.Runtime.getRuntime
            (runtime.totalMemory - runtime.freeMemory) >> 10
          elsif windows?
            # not yet
          else
            `ps -o rss= -p #{Process.pid}`.to_i
          end
        end
      end # Methods

    end # System
  end # Helper
end # Spark
