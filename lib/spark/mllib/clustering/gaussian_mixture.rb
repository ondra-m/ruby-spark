module Spark
  module Mllib
    ##
    # GaussianMixtureModel
    #
    # A clustering model derived from the Gaussian Mixture Model method.
    #
    # == Examples:
    #
    #   Spark::Mllib.import
    #
    #   data = [
    #     DenseVector.new([-0.1, -0.05]),
    #     DenseVector.new([-0.01, -0.1]),
    #     DenseVector.new([0.9, 0.8]),
    #     DenseVector.new([0.75, 0.935]),
    #     DenseVector.new([-0.83, -0.68]),
    #     DenseVector.new([-0.91, -0.76])
    #   ]
    #
    #   model = GaussianMixture.train($sc.parallelize(data), 3, convergence_tol: 0.0001, max_iterations: 50, seed: 10)
    #
    #   labels = model.predict($sc.parallelize(data)).collect
    #
    class GaussianMixtureModel

      attr_reader :weights, :gaussians, :k

      def initialize(weights, gaussians)
        @weights = weights
        @gaussians = gaussians
        @k = weights.size
      end

      # Find the cluster to which the points in 'x' has maximum membership
      # in this model.
      def predict(rdd)
        if rdd.is_a?(Spark::RDD)
          predict_soft(rdd).map('lambda{|x| x.index(x.max)}')
        else
          raise ArgumentError, 'Argument must be a RDD.'
        end
      end

      # Find the membership of each point in 'x' to all mixture components.
      def predict_soft(rdd)
        Spark.jb.call(RubyMLLibAPI.new, 'predictSoftGMM', rdd, weights, means, sigmas)
      end

      def means
        @means ||= @gaussians.map(&:mu)
      end

      def sigmas
        @sigmas ||= @gaussians.map(&:sigma)
      end

    end
  end
end

module Spark
  module Mllib
    class GaussianMixture

      def self.train(rdd, k, convergence_tol: 0.001, max_iterations: 100, seed: nil)
        weights, means, sigmas = Spark.jb.call(RubyMLLibAPI.new, 'trainGaussianMixtureModel', rdd,
                                               k, convergence_tol, max_iterations, Spark.jb.to_long(seed))

        means.map! {|mu|    Spark.jb.java_to_ruby(mu)}
        sigmas.map!{|sigma| Spark.jb.java_to_ruby(sigma)}

        mvgs = Array.new(k) do |i|
          MultivariateGaussian.new(means[i], sigmas[i])
        end

        GaussianMixtureModel.new(weights, mvgs)
      end

    end
  end
end
