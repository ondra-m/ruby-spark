module Spark
  module Serializer
    autoload :Base,        "spark/serializer/base" # DO NOT USE IT
    autoload :UTF8,        "spark/serializer/utf8"
    autoload :Marshal,     "spark/serializer/marshal"
    autoload :MessagePack, "spark/serializer/message_pack"
    autoload :Oj,          "spark/serializer/oj"
    autoload :Pair,        "spark/serializer/pair"
    autoload :Cartesian,   "spark/serializer/cartesian"

    DEFAULT_BATCH_SIZE = 1024
    DEFAULT_SERIALIZER_NAME = "marshal"

    def self.get(suggestion)
      const_get(suggestion.to_s.camelize) rescue nil
    end

    def self.get!(suggestion)
      const_get(suggestion.to_s.camelize)
    rescue
      raise Spark::NotImplemented, "Serializer #{suggestion.to_s.camelize} not exist."
    end
  end
end
