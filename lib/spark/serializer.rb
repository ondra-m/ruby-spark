module Spark
  module Serializer
    autoload :Base,        "spark/serializer/base" # DO NOT USE IT
    autoload :Helper,      "spark/serializer/helper"
    autoload :UTF8,        "spark/serializer/utf8"
    autoload :Marshal,     "spark/serializer/marshal"
    autoload :MessagePack, "spark/serializer/message_pack"
  end
end
