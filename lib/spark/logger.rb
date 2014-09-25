# Necessary libraries
Spark.load_lib

module Spark
  class Logger

    def self.logger
      JLogger.getLogger("Ruby")
    end

    def self.level_off
      JLevel.toLevel("OFF")
    end

    # Disable all Spark log
    def self.disable
      logger.setLevel(level_off)
      JLogger.getLogger("org").setLevel(level_off)
      JLogger.getLogger("akka").setLevel(level_off)
      JLogger.getRootLogger.setLevel(level_off)
    end

    def self.enabled?
      !disabled?
    end

    def self.info(message)
      logger.info(message) if info?
    end

    def self.debug(message)
      logger.debug(message) if debug?
    end

    def self.trace(message)
      logger.trace(message) if trace?
    end

    def self.warning(message)
      logger.warn(message) if warning?
    end

    def self.error(message)
      logger.error(message) if error?
    end

    def self.info?
      level_enabled?("info")
    end

    def self.debug?
      level_enabled?("debug")
    end

    def self.trace?
      level_enabled?("trace")
    end

    def self.warning?
      level_enabled?("warn")
    end

    def self.error?
      level_enabled?("error")
    end

    def self.level_enabled?(type)
      logger.isEnabledFor(JPriority.toPriority(type.upcase))
    end

  end
end
