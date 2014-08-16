module Spark
  module Serializer
    autoload :Base,     "spark/serializer/base" # DO NOT USE IT
    autoload :UTF8,     "spark/serializer/utf8"
    autoload :Simple,   "spark/serializer/simple"
    autoload :Helper,   "spark/serializer/helper"
  end
end
