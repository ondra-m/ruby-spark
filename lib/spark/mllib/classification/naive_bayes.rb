module Spark
  module Mllib
    ##
    # NaiveBayesModel
    #
    # Model for Naive Bayes classifiers.
    #
    # Contains two parameters:
    # pi:: vector of logs of class priors (dimension C)
    # theta:: matrix of logs of class conditional probabilities (CxD)
    #
    # == Examples:
    #
    #   Spark::Mllib.import
    #
    #   # Dense vectors
    #   data = [
    #     LabeledPoint.new(0.0, [0.0, 0.0]),
    #     LabeledPoint.new(0.0, [0.0, 1.0]),
    #     LabeledPoint.new(1.0, [1.0, 0.0])
    #   ]
    #   model = NaiveBayes.train($sc.parallelize(data))
    #
    #   model.predict([0.0, 1.0])
    #   # => 0.0
    #   model.predict([1.0, 0.0])
    #   # => 1.0
    #
    #
    #   # Sparse vectors
    #   data = [
    #     LabeledPoint.new(0.0, SparseVector.new(2, {1 => 0.0})),
    #     LabeledPoint.new(0.0, SparseVector.new(2, {1 => 1.0})),
    #     LabeledPoint.new(1.0, SparseVector.new(2, {0 => 1.0}))
    #   ]
    #   model = NaiveBayes.train($sc.parallelize(data))
    #
    #   model.predict(SparseVector.new(2, {1 => 1.0}))
    #   # => 0.0
    #   model.predict(SparseVector.new(2, {0 => 1.0}))
    #   # => 1.0
    #
    class NaiveBayesModel

      attr_reader :labels, :pi, :theta

      def initialize(labels, pi, theta)
        @labels = labels
        @pi = pi
        @theta = theta
      end

      # Predict values for a single data point or an RDD of points using
      # the model trained.
      def predict(vector)
        vector = Spark::Mllib::Vectors.to_vector(vector)
        array = (vector.dot(theta) + pi).to_a
        index = array.index(array.max)
        labels[index]
      end

    end
  end
end


module Spark
  module Mllib
    class NaiveBayes

      # Trains a Naive Bayes model given an RDD of (label, features) pairs.
      #
      # This is the Multinomial NB (http://tinyurl.com/lsdw6p) which can handle all kinds of
      # discrete data.  For example, by converting documents into TF-IDF vectors, it can be used for
      # document classification.  By making every vector a 0-1 vector, it can also be used as
      # Bernoulli NB (http://tinyurl.com/p7c96j6). The input feature values must be nonnegative.
      #
      # == Arguments:
      # rdd:: RDD of LabeledPoint.
      # lambda:: The smoothing parameter.
      #
      def self.train(rdd, lambda=1.0)
        # Validation
        first = rdd.first
        unless first.is_a?(LabeledPoint)
          raise Spark::MllibError, "RDD should contains LabeledPoint, got #{first.class}"
        end

        labels, pi, theta = Spark.jb.call(RubyMLLibAPI.new, 'trainNaiveBayesModel', rdd, lambda)
        theta = Spark::Mllib::Matrices.dense(theta.size, theta.first.size, theta)

        NaiveBayesModel.new(labels, pi, theta)
      end

    end
  end
end
