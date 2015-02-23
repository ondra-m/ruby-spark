##
# The features and labels of a data point.
#
# == Parameters:
# label::
#   Label for this data point.
#
# features::
#   Vector of features for this point
#
module Spark
  module Mllib
    class LabeledPoint

      include Spark::Helper::Serialize

      attr_reader :label, :features

      def initialize(label, features)
        @label = label
        @features = Spark::Mllib::Vector.to_vector(features)
      end

      def _dump(depth)
        pack_long(@label) + @features._dump
      end

      def self._load(data)
        puts data
      end

    end
  end
end

module Spark
  module Mllib
    class LinearRegressionModel
      def initialize(*)
      end
    end
  end
end


module Spark
  module Mllib
    class LinearRegressionWithSGD

      def self.train(rdd, iterations: 100, step: 1.0, mini_batch_fraction: 1.0,
                          initial_weights: nil, reg_param: 0.0, reg_type: nil,
                          intercept: false)


        initial_weights = Vector.init_from(initial_weights || [0.0] * first.features.size)

        weights, intercept = Spark.jb.call(RubyMLLibAPI, 'trainLinearRegressionModelWithSGD', false,
                                           rdd, iterations.to_i, step.to_f, mini_batch_fraction.to_f,
                                           initial_weights, reg_param.to_f, reg_type, intercept)

        # LinearRegressionModel.new(weights, intercept)
      end

    end
  end
end
