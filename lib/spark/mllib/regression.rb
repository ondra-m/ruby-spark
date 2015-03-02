##
# LabeledPoint
#
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
        @label = label.to_f
        @features = Spark::Mllib::Vector.to_vector(features)
      end

      def self.from_java(object)
        LabeledPoint.new(
          object.label,
          Spark.jb.java_to_ruby(object.features)
        )
      end

      def marshal_dump
        [@label, @features]
      end

      def marshal_load(array)
        initialize(array[0], array[1])
      end

    end
  end
end

##
# LinearModel
#
# A linear model that has a vector of coefficients and an intercept.
#
module Spark
  module Mllib
    class LinearModel

      attr_reader :weights, :intercept

      def initialize(weights, intercept)
        @weights = Spark::Mllib::Vector.to_vector(weights)
        @intercept = intercept.to_f
      end

      # Predict the value of the dependent variable given a vector data
      # containing values for the independent variables.
      #
      # == Examples:
      #
      # lm = LinearModel.new([1.0, 2.0], 0.1)
      #
      # lm.predict([-1.03, 7.777]) - 14.624 < 1e-6
      # => true
      #
      # lm.predict(SparseVector.new(2, {0 => -1.03, 1 => 7.777})) - 14.624 < 1e-6
      # => true
      #
      def predict(data)
        data = Spark::Mllib::Vector.to_vector(data)
        @weights.dot(data) + @intercept
      end

    end
  end
end



# =======================================================================================
# Linear regression

class Spark::Mllib::LinearRegressionModel < Spark::Mllib::LinearModel
end

##
# LinearRegressionWithSGD
#
# Train a linear regression model with no regularization using Stochastic Gradient Descent.
# This solves the least squares regression formulation
#              f(weights) = 1/n ||A weights-y||^2^
# (which is the mean squared error).
# Here the data matrix has n rows, and the input RDD holds the set of rows of A, each with
# its corresponding right hand side label y.
# See also the documentation for the precise formulation.
#
# == Examples:
#
#   Spark::Mllib.load
#
#   # Dense vectors
#   data = [
#     LabeledPoint.new(0.0, [0.0]),
#     LabeledPoint.new(1.0, [1.0]),
#     LabeledPoint.new(3.0, [2.0]),
#     LabeledPoint.new(2.0, [3.0])
#   ]
#   lrm = LinearRegressionWithSGD.train($sc.parallelize(data), initial_weights: [1.0])
#
#   lrm.intercept # => 0.0
#   lrm.weights   # => [0.9285714285714286]
#
#   lrm.predict([0.0]) - 0 < 0.5
#   # => true
#
#   lrm.predict([1.0]) - 1 < 0.5
#   # => true
#
#   lrm.predict(SparseVector.new(1, {0 => 1.0})) - 1 < 0.5
#   # => true
#
#   # Sparse vectors
#   data = [
#     LabeledPoint.new(0.0, SparseVector.new(1, {0 => 0.0})),
#     LabeledPoint.new(1.0, SparseVector.new(1, {0 => 1.0})),
#     LabeledPoint.new(3.0, SparseVector.new(1, {0 => 2.0})),
#     LabeledPoint.new(2.0, SparseVector.new(1, {0 => 3.0}))
#   ]
#   lrm = LinearRegressionWithSGD.train($sc.parallelize(data), initial_weights: [1.0])
#
#   lrm.intercept # => 0.0
#   lrm.weights   # => [0.9285714285714286]
#
#   lrm.predict([0.0]) - 0 < 0.5
#   # => true
#
#   lrm.predict(SparseVector.new(1, {0 => 1.0})) - 1 < 0.5
#   # => true
#
module Spark
  module Mllib
    class LinearRegressionWithSGD

      # Train a linear regression model on the given data.
      #
      # == Parameters:
      # rdd::
      #   The training data (RDD instance).
      #
      # iterations::
      #   The number of iterations (default: 100).
      #
      # step::
      #   The step parameter used in SGD (default: 1.0).
      #
      # mini_batch_fraction::
      #   Fraction of data to be used for each SGD iteration (default: 1.0).
      #
      # initial_weights::
      #   The initial weights (default: nil).
      #
      # reg_param::
      #   The regularizer parameter (default: 0.0).
      #
      # reg_type::
      #   The type of regularizer used for training our model (default: nil).
      #
      #   Allowed values:
      #     - "l1" for using L1 regularization (lasso),
      #     - "l2" for using L2 regularization (ridge),
      #     - None for no regularization
      #
      # intercept::
      #   Boolean parameter which indicates the use
      #   or not of the augmented representation for
      #   training data (i.e. whether bias features
      #   are activated or not). (default: False)
      #
      def self.train(rdd, iterations: 100, step: 1.0, mini_batch_fraction: 1.0,
                          initial_weights: nil, reg_param: 0.0, reg_type: nil,
                          intercept: false)

        first = rdd.first
        unless first.is_a?(LabeledPoint)
          raise Spark::MllibError, "RDD should contains LabeledPoint, got #{first.class}"
        end

        initial_weights = Vector.to_vector(initial_weights || [0.0] * first.features.size)

        weights, intercept = Spark.jb.call(RubyMLLibAPI.new, 'trainLinearRegressionModelWithSGD',
                                           rdd, iterations.to_i, step.to_f, mini_batch_fraction.to_f,
                                           initial_weights, reg_param.to_f, reg_type, intercept)

        LinearRegressionModel.new(weights, intercept)
      end

    end
  end
end



# =======================================================================================
# Lasso regression

class Spark::Mllib::LassoModel < Spark::Mllib::LinearModel
end

##
# LassoWithSGD
#
# Train a regression model with L1-regularization using Stochastic Gradient Descent.
# This solves the l1-regularized least squares regression formulation
#          f(weights) = 1/2n ||A weights-y||^2^  + regParam ||weights||_1
# Here the data matrix has n rows, and the input RDD holds the set of rows of A, each with
# its corresponding right hand side label y.
# See also the documentation for the precise formulation.
#
# == Examples:
#
#   Spark::Mllib.load
#
#   # Dense vectors
#   data = [
#       LabeledPoint.new(0.0, [0.0]),
#       LabeledPoint.new(1.0, [1.0]),
#       LabeledPoint.new(3.0, [2.0]),
#       LabeledPoint.new(2.0, [3.0])
#   ]
#   lrm = LassoWithSGD.train($sc.parallelize(data), initial_weights: [1.0])
#
#   lrm.predict([0.0]) - 0 < 0.5
#   # => true
#
#   lrm.predict([1.0]) - 1 < 0.5
#   # => true
#
#   lrm.predict(SparseVector.new(1, {0 => 1.0})) - 1 < 0.5
#   # => true
#
#
#   # Sparse vectors
#   data = [
#       LabeledPoint.new(0.0, SparseVector.new(1, {0 => 0.0})),
#       LabeledPoint.new(1.0, SparseVector.new(1, {0 => 1.0})),
#       LabeledPoint.new(3.0, SparseVector.new(1, {0 => 2.0})),
#       LabeledPoint.new(2.0, SparseVector.new(1, {0 => 3.0}))
#   ]
#   lrm = LinearRegressionWithSGD.train($sc.parallelize(data), initial_weights: [1.0])
#
#   lrm.predict([0.0]) - 0 < 0.5
#   # => true
#
#   lrm.predict(SparseVector.new(1, {0 => 1.0})) - 1 < 0.5
#   # => true
#
module Spark
  module Mllib
    class LassoWithSGD

      # Train a Lasso regression model on the given data.
      #
      # == Parameters:
      # rdd::
      #   The training data (RDD instance).
      #
      # iterations::
      #   The number of iterations (default: 100).
      #
      # step::
      #   The step parameter used in SGD (default: 1.0).
      #
      # reg_param::
      #   The regularizer parameter (default: 0.0).
      #
      # mini_batch_fraction::
      #   Fraction of data to be used for each SGD iteration (default: 1.0).
      #
      # initial_weights::
      #   The initial weights (default: nil).
      #
      def self.train(rdd, iterations: 100, step: 1.0, reg_param: 0.01, mini_batch_fraction: 1.0, initial_weights: nil)
        first = rdd.first
        unless first.is_a?(LabeledPoint)
          raise Spark::MllibError, "RDD should contains LabeledPoint, got #{first.class}"
        end

        first = rdd.first
        initial_weights = Vector.to_vector(initial_weights || [0.0] * first.features.size)

        weights, intercept = Spark.jb.call(RubyMLLibAPI.new, 'trainLassoModelWithSGD',
                                           rdd, iterations.to_i, step.to_f, reg_param.to_f,
                                           mini_batch_fraction.to_f, initial_weights)

        LassoModel.new(weights, intercept)
      end

    end
  end
end



# =======================================================================================
# Ridge regression

class Spark::Mllib::RidgeRegressionModel < Spark::Mllib::LinearModel
end

##
# RidgeRegressionWithSGD
#
# Train a regression model with L2-regularization using Stochastic Gradient Descent.
# This solves the l1-regularized least squares regression formulation
#          f(weights) = 1/2n ||A weights-y||^2^  + regParam/2 ||weights||^2^
# Here the data matrix has n rows, and the input RDD holds the set of rows of A, each with
# its corresponding right hand side label y.
# See also the documentation for the precise formulation.
#
# == Examples:
#
#   Spark::Mllib.load
#
#   data = [
#       LabeledPoint.new(0.0, [0.0]),
#       LabeledPoint.new(1.0, [1.0]),
#       LabeledPoint.new(3.0, [2.0]),
#       LabeledPoint.new(2.0, [3.0])
#   ]
#   lrm = RidgeRegressionWithSGD.train($sc.parallelize(data), initial_weights: [1.0])
#
#   lrm.predict([0.0]) - 0 < 0.5
#   # => true
#
#   lrm.predict([1.0]) - 1 < 0.5
#   # => true
#
#   lrm.predict(SparseVector.new(1, {0 => 1.0})) - 1 < 0.5
#   # => true
#
#   data = [
#       LabeledPoint.new(0.0, SparseVector.new(1, {0 => 0.0})),
#       LabeledPoint.new(1.0, SparseVector.new(1, {0 => 1.0})),
#       LabeledPoint.new(3.0, SparseVector.new(1, {0 => 2.0})),
#       LabeledPoint.new(2.0, SparseVector.new(1, {0 => 3.0}))
#   ]
#   lrm = LinearRegressionWithSGD.train($sc.parallelize(data), initial_weights: [1.0])
#
#   lrm.predict([0.0]) - 0 < 0.5
#   # => true
#
#   lrm.predict(SparseVector.new(1, {0 => 1.0})) - 1 < 0.5
#   # => true
#
module Spark
  module Mllib
    class RidgeRegressionWithSGD

      # Train a ridge regression model on the given data.
      #
      # == Parameters:
      # rdd::
      #   The training data (RDD instance).
      #
      # iterations::
      #   The number of iterations (default: 100).
      #
      # step::
      #   The step parameter used in SGD (default: 1.0).
      #
      # reg_param::
      #   The regularizer parameter (default: 0.0).
      #
      # mini_batch_fraction::
      #   Fraction of data to be used for each SGD iteration (default: 1.0).
      #
      # initial_weights::
      #   The initial weights (default: nil).
      #
      def self.train(rdd, iterations: 100, step: 1.0, reg_param: 0.01, mini_batch_fraction: 1.0, initial_weights: nil)
        first = rdd.first
        unless first.is_a?(LabeledPoint)
          raise Spark::MllibError, "RDD should contains LabeledPoint, got #{first.class}"
        end

        first = rdd.first
        initial_weights = Vector.to_vector(initial_weights || [0.0] * first.features.size)

        weights, intercept = Spark.jb.call(RubyMLLibAPI.new, 'trainRidgeModelWithSGD',
                                           rdd, iterations.to_i, step.to_f, reg_param.to_f,
                                           mini_batch_fraction.to_f, initial_weights)

        RidgeRegressionModel.new(weights, intercept)
      end

    end
  end
end
