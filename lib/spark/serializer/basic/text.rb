module Spark
  class Serializer
    ##
    # Text
    #
    # This class should be used only for loading data from io
    #
    class Text

      attr_reader :encoding

      def initialize(encoding=Encoding::UTF_8)
        unless encoding.is_a?(Encoding)
          raise Spark::SerializeError, 'Encoding must be an instance of Encoding'
        end

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
