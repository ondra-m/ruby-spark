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
    class LassoWithSGD < RegressionMethodBase

      DEFAULT_OPTIONS = {
        iterations: 100,
        step: 1.0,
        reg_param: 0.01,
        mini_batch_fraction: 1.0,
        initial_weights: nil
      }

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
      def self.train(rdd, options={})
        super

        weights, intercept = Spark.jb.call(RubyMLLibAPI.new, 'trainLassoModelWithSGD', rdd,
                                           options[:iterations].to_i,
                                           options[:step].to_f,
                                           options[:reg_param].to_f,
                                           options[:mini_batch_fraction],
                                           options[:initial_weights].to_f)

        LassoModel.new(weights, intercept)
      end

    end
  end
end
