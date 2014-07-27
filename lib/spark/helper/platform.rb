module Spark
  module Helper
    module Platform

      def windows?
        RbConfig::CONFIG['host_os'] =~ /mswin|mingw/
      end

      def mri?
        RbConfig::CONFIG['ruby_install_name'] == 'ruby'
      end

      def jruby?
        RbConfig::CONFIG['ruby_install_name'] == 'jruby'
      end

    end
  end
end
