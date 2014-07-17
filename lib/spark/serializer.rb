module Spark
  module Serializer
    autoload :UTF8,    "spark/serializer/utf8"
    autoload :Simple,  "spark/serializer/simple"
    autoload :Batched, "spark/serializer/batched"
  end
end
