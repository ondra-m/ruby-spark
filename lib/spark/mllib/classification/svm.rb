module Spark
  module Mllib
    ##
    # SVMModel
    #
    # A support vector machine.
    #
    # == Examples:
    #
    #   Spark::Mllib.import
    #
    #   # Dense vectors
    #   data = [
    #       LabeledPoint.new(0.0, [0.0]),
    #       LabeledPoint.new(1.0, [1.0]),
    #       LabeledPoint.new(1.0, [2.0]),
    #       LabeledPoint.new(1.0, [3.0])
    #   ]
    #   svm = SVMWithSGD.train($sc.parallelize(data))
    #
    #   svm.predict([1.0])
    #   # => 1
    #   svm.clear_threshold
    #   svm.predict([1.0])
    #   # => 1.25...
    #
    #
    #   # Sparse vectors
    #   data = [
    #       LabeledPoint.new(0.0, SparseVector.new(2, {0 => -1.0})),
    #       LabeledPoint.new(1.0, SparseVector.new(2, {1 => 1.0})),
    #       LabeledPoint.new(0.0, SparseVector.new(2, {0 => 0.0})),
    #       LabeledPoint.new(1.0, SparseVector.new(2, {1 => 2.0}))
    #   ]
    #   svm = SVMWithSGD.train($sc.parallelize(data))
    #
    #   svm.predict(SparseVector.new(2, {1 => 1.0}))
    #   # => 1
    #   svm.predict(SparseVector.new(2, {0 => -1.0}))
    #   # => 0
    #
    class SVMModel < ClassificationModel

      def initialize(*args)
        super
        @threshold = 0.0
      end

      # Predict values for a single data point or an RDD of points using
      # the model trained.
      def predict(vector)
        vector = Spark::Mllib::Vectors.to_vector(vector)
        margin = weights.dot(vector) + intercept

        if threshold.nil?
          return margin
        end

        if margin > threshold
          1
        else
          0
        end
      end

    end
  end
end

module Spark
  module Mllib
    class SVMWithSGD < ClassificationMethodBase

      DEFAULT_OPTIONS = {
        iterations: 100,
        step: 1.0,
        reg_param: 0.01,
        mini_batch_fraction: 1.0,
        initial_weights: nil,
        reg_type: 'l2',
        intercept: false,
        validate: true,
        convergence_tol: 0.001
      }

      # Train a support vector machine on the given data.
      #
      # rdd::
      #   The training data, an RDD of LabeledPoint.
      #
      # iterations::
      #   The number of iterations (default: 100).
      #
      # step::
      #   The step parameter used in SGD (default: 1.0).
      #
      # reg_param::
      #   The regularizer parameter (default: 0.01).
      #
      # mini_batch_fraction::
      #   Fraction of data to be used for each SGD iteration.
      #
      # initial_weights::
      #   The initial weights (default: nil).
      #
      # reg_type::
      #   The type of regularizer used for training our model (default: "l2").
      #
      #   Allowed values:
      #   - "l1" for using L1 regularization
      #   - "l2" for using L2 regularization
      #   - nil for no regularization
      #
      # intercept::
      #   Boolean parameter which indicates the use
      #   or not of the augmented representation for
      #   training data (i.e. whether bias features
      #   are activated or not).
      #   (default: false)
      #
      # validateData::
      #   Boolean parameter which indicates if the
      #   algorithm should validate data before training.
      #   (default: true)
      #
      # convergence_tol::
      #   A condition which decides iteration termination.
      #   (default: 0.001)
      #
      def self.train(rdd, options={})
        super

        weights, intercept = Spark.jb.call(RubyMLLibAPI.new, 'trainSVMModelWithSGD', rdd,
                                           options[:iterations].to_i,
                                           options[:step].to_f,
                                           options[:reg_param].to_f,
                                           options[:mini_batch_fraction].to_f,
                                           options[:initial_weights],
                                           options[:reg_type],
                                           options[:intercept],
                                           options[:validate],
                                           options[:convergence_tol])

        SVMModel.new(weights, intercept)
      end

    end
  end
end
