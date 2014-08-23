module Spark
  module Serializer
    autoload :Base,        "spark/serializer/base" # DO NOT USE IT
    autoload :Helper,      "spark/serializer/helper"
    autoload :UTF8,        "spark/serializer/utf8"
    autoload :Marshal,     "spark/serializer/marshal"
    autoload :MessagePack, "spark/serializer/message_pack"
    autoload :Oj,          "spark/serializer/oj"

    DEFAULT_BATCH_SIZE = 1024
    DEFAULT_SERIALIZER_NAME = "marshal"

    def self.camelize(text)
      text.to_s.gsub(/\/(.?)/) { "::#{$1.upcase}" }.gsub(/(?:^|_)(.)/) { $1.upcase }
    end

    def self.get(suggestion)
      const_get(camelize(suggestion)) rescue nil
    end

    def self.get!(suggestion)
      const_get(camelize(suggestion))
    rescue
      raise Spark::NotImplemented, "Serializer #{camelize(suggestion)} not exist."
    end
  end
end
