module Spark
  module Helper
    module Platform

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

        def config_for_java
          hash_map = HashMap.new
          RbConfig::CONFIG.each_pair {|key, value| hash_map.put(key, value)}
          hash_map
        end
      end # Methods

    end # Platform
  end # Helper
end # Spark
