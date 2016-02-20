##
# LinearRegressionModel
#
# Train a linear regression model with no regularization using Stochastic Gradient Descent.
# This solves the least squares regression formulation
#   f(weights) = 1/n ||A weights-y||^2^
# (which is the mean squared error).
# Here the data matrix has n rows, and the input RDD holds the set of rows of A, each with
# its corresponding right hand side label y.
# See also the documentation for the precise formulation.
#
# == Examples:
#
#   Spark::Mllib.import
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
#   lrm.predict([0.0]) < 0.5
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
#   lrm.predict([0.0]) < 0.5
#   # => true
#
#   lrm.predict(SparseVector.new(1, {0 => 1.0})) - 1 < 0.5
#   # => true
#
class Spark::Mllib::LinearRegressionModel < Spark::Mllib::RegressionModel
end

module Spark
  module Mllib
    class LinearRegressionWithSGD < RegressionMethodBase

      DEFAULT_OPTIONS = {
        iterations: 100,
        step: 1.0,
        mini_batch_fraction: 1.0,
        initial_weights: nil,
        reg_param: 0.0,
        reg_type: nil,
        intercept: false,
        validate: true,
        convergence_tol: 0.001
      }

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
      #   - "l1" for using L1 regularization (lasso),
      #   - "l2" for using L2 regularization (ridge),
      #   - None for no regularization
      #
      # intercept::
      #   Boolean parameter which indicates the use
      #   or not of the augmented representation for
      #   training data (i.e. whether bias features
      #   are activated or not).
      #   (default: false)
      #
      # validate::
      #   Boolean parameter which indicates if the
      #   algorithm should validate data before training.
      #   (default: true)
      #
      # convergence_tol::
      #    A condition which decides iteration termination.
      #    (default: 0.001)
      #
      def self.train(rdd, options={})
        super

        weights, intercept = Spark.jb.call(RubyMLLibAPI.new, 'trainLinearRegressionModelWithSGD', rdd,
                                           options[:iterations].to_i,
                                           options[:step].to_f,
                                           options[:mini_batch_fraction].to_f,
                                           options[:initial_weights],
                                           options[:reg_param].to_f,
                                           options[:reg_type],
                                           options[:intercept],
                                           options[:validate],
                                           options[:convergence_tol])

        LinearRegressionModel.new(weights, intercept)
      end

    end
  end
end
