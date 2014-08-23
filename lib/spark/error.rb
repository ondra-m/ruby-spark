module Spark
  # Extension cannot be built
  class BuildError < StandardError
  end

  # Proc.to_source
  class SerializeError < StandardError
  end

  # Serializer method
  # Non-existing serializer
  class NotImplemented < StandardError
  end

  # Missison app_name or master
  class ConfigurationError < StandardError
  end
end
