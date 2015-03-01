require 'mdarray'

module Spark
  module Mllib
    class BaseVector < MDArray

      def dot(vector)
        raise NotImplementedError, 'dot'
      end

    end
  end
end
