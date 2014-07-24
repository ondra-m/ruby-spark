module Spark
  module Command
    class Template
      
      attr_accessor :serializer, :deserializer, :stages, :library, :pre

      def initialize
        @serializer = @deserializer = nil
        @stages, @library, @pre = [], [], []
      end

    end
  end
end

module Spark
  module Command
    class Stage
      
      attr_accessor :pre, :main

      def initialize
        @main = nil
        @pre = []
      end

    end
  end
end
