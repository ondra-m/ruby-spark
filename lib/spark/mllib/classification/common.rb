module Spark
  module Mllib
    class ClassificationModel

      attr_reader :weights, :intercept, :threshold

      def initialize(weights, intercept)
        @weights = Spark::Mllib::Vectors.to_vector(weights)
        @intercept = intercept.to_f
        @threshold = nil
      end

      def threshold=(value)
        @threshold = value.to_f
      end

      def clear_threshold
        @threshold = nil
      end

    end
  end
end

module Spark
  module Mllib
    class ClassificationMethodBase < RegressionMethodBase

    end
  end
end
