module Spark
  module Serializer
    class Text < Base

      attr_reader :encoding

      def initialize(encoding=Encoding::UTF_8)
        error('Encoding must be an instance of Encoding') unless encoding.is_a?(Encoding)

        @encoding = encoding
      end

      def load(data)
        data.to_s.force_encoding(@encoding)
      end

      def to_s
        "Text(#{@encoding})"
      end

    end
  end
end

Spark::Serializer.register('string', 'text', Spark::Serializer::Text)
