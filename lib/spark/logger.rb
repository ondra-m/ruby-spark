# Necessary libraries
Spark.load_lib

module Spark
  class Logger

    attr_reader :jlogger

    def initialize
      @jlogger = JLogger.getLogger('Ruby')
    end

    def level_off
      JLevel.toLevel('OFF')
    end

    # Disable all Spark log
    def disable
      jlogger.setLevel(level_off)
      JLogger.getLogger('org').setLevel(level_off)
      JLogger.getLogger('akka').setLevel(level_off)
      JLogger.getRootLogger.setLevel(level_off)
    end

    def enabled?
      !disabled?
    end

    def info(message)
      jlogger.info(message) if info?
    end

    def debug(message)
      jlogger.debug(message) if debug?
    end

    def trace(message)
      jlogger.trace(message) if trace?
    end

    def warning(message)
      jlogger.warn(message) if warning?
    end

    def error(message)
      jlogger.error(message) if error?
    end

    def info?
      level_enabled?('info')
    end

    def debug?
      level_enabled?('debug')
    end

    def trace?
      level_enabled?('trace')
    end

    def warning?
      level_enabled?('warn')
    end

    def error?
      level_enabled?('error')
    end

    def level_enabled?(type)
      jlogger.isEnabledFor(JPriority.toPriority(type.upcase))
    end

    alias_method :warn, :warning

  end
end
