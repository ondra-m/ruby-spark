module Spark
  module Mllib
    ##
    # KMeansModel
    #
    # A clustering model derived from the k-means method.
    #
    # == Examples:
    #
    #   Spark::Mllib.import
    #
    #   # Dense vectors
    #   data = [
    #     DenseVector.new([0.0,0.0]),
    #     DenseVector.new([1.0,1.0]),
    #     DenseVector.new([9.0,8.0]),
    #     DenseVector.new([8.0,9.0])
    #   ]
    #
    #   model = KMeans.train($sc.parallelize(data), 2, max_iterations: 10,
    #                        runs: 30, initialization_mode: "random")
    #
    #   model.predict([0.0, 0.0]) == model.predict([1.0, 1.0])
    #   # => true
    #   model.predict([8.0, 9.0]) == model.predict([9.0, 8.0])
    #   # => true
    #
    #
    #   # Sparse vectors
    #   data = [
    #       SparseVector.new(3, {1 => 1.0}),
    #       SparseVector.new(3, {1 => 1.1}),
    #       SparseVector.new(3, {2 => 1.0}),
    #       SparseVector.new(3, {2 => 1.1})
    #   ]
    #   model = KMeans.train($sc.parallelize(data), 2, initialization_mode: "k-means||")
    #
    #   model.predict([0.0, 1.0, 0.0]) == model.predict([0, 1.1, 0.0])
    #   # => true
    #   model.predict([0.0, 0.0, 1.0]) == model.predict([0, 0, 1.1])
    #   # => true
    #   model.predict(data[0]) == model.predict(data[1])
    #   # => true
    #   model.predict(data[2]) == model.predict(data[3])
    #   # => true
    #
    class KMeansModel

      attr_reader :centers

      def initialize(centers)
        @centers = centers
      end

      # Find the cluster to which x belongs in this model.
      def predict(vector)
        vector = Spark::Mllib::Vectors.to_vector(vector)
        best = 0
        best_distance = Float::INFINITY

        @centers.each_with_index do |center, index|
          distance = vector.squared_distance(center)
          if distance < best_distance
            best = index
            best_distance = distance
          end
        end

        best
      end

      def self.from_java(object)
        centers = object.clusterCenters
        centers.map! do |center|
          Spark.jb.java_to_ruby(center)
        end

        KMeansModel.new(centers)
      end

    end
  end
end

module Spark
  module Mllib
    class KMeans

      # Trains a k-means model using the given set of parameters.
      #
      # == Arguments:
      # rdd::
      #   The training data, an RDD of Vectors.
      #
      # k::
      #   Number of clusters.
      #
      # max_iterations::
      #   Max number of iterations.
      #
      # runs::
      #   Number of parallel runs, defaults to 1. The best model is returned.
      #
      # initialization_mode::
      #   Initialization model, either "random" or "k-means||" (default).
      #
      # seed::
      #   Random seed value for cluster initialization.
      #
      # epsilon::
      #   The distance threshold within which we've consider centers to have converged.
      #
      def self.train(rdd, k, max_iterations: 100, runs: 1, initialization_mode: 'k-means||', seed: nil,
                             initialization_steps: 5, epsilon: 0.0001)

        cluster_initial_model = []

        # Call returns KMeansModel
        Spark.jb.call(RubyMLLibAPI.new, 'trainKMeansModel', rdd,
                      k, max_iterations, runs, initialization_mode, Spark.jb.to_long(seed), initialization_steps, epsilon, cluster_initial_model)
      end

    end
  end
end
