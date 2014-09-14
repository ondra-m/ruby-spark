module Spark
  module Helper
    module Logger

      def self.included(base)
        base.send :extend,  Methods
        base.send :include, Methods
      end
     
      module Methods
        def log_info(message)
          Spark::Logger.info(message)
        end

        def log_debug(message)
          Spark::Logger.debug(message)
        end

        def log_trace(message)
          Spark::Logger.trace(message)
        end

        def log_warning(message)
          Spark::Logger.warning(message)
        end

        def log_error(message)
          Spark::Logger.error(message)
        end

        alias_method :logInfo,    :log_info
        alias_method :logDebug,   :log_debug
        alias_method :logTrace,   :log_trace
        alias_method :logWarning, :log_warning
        alias_method :logError,   :log_error

      end # Methods
    end # Logger
  end # Helper
end # Spark
