module Spark
  # Extension cannot be built
  class BuildError < StandardError
  end

  # Proc.to_source
  # Java object cannot be converted
  class SerializeError < StandardError
  end

  # Serializer method
  # Non-existing serializer
  class NotImplemented < StandardError
  end

  # Missison app_name or master
  class ConfigurationError < StandardError
  end

  # Wrong parameters
  class RDDError < StandardError
  end

  # Validations
  class CommandError < StandardError
  end

  # Parser helper
  # SQL DataType
  class ParseError < StandardError
  end

  # Validation in context
  class ContextError < StandardError
  end

  # Broadcasts
  # Missing path
  class BroadcastError < StandardError
  end

  # Accumulators
  # Existing keys
  # Wrong ID
  class AccumulatorError < StandardError
  end

  # Wrong instances
  class MllibError < StandardError
  end

  # Wrong datatype
  class SQLError < StandardError
  end

  # Missing Java class
  class JavaBridgeError < StandardError
  end
end
