module Spark
  class BuildError < StandardError
  end

  # Proc.to_source
  class SerializeError < StandardError
  end

  # Serializer method
  class NotImplemented < StandardError
  end
end
