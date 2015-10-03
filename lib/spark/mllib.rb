module Spark
  # MLlib is Spark’s scalable machine learning library consisting of common learning algorithms and utilities,
  # including classification, regression, clustering, collaborative filtering, dimensionality reduction,
  # as well as underlying optimization primitives.
  module Mllib
    extend Spark::Library

    # Base classes
    autoload_without_import :VectorBase, 'spark/mllib/vector'
    autoload_without_import :MatrixBase, 'spark/mllib/matrix'
    autoload_without_import :RegressionMethodBase,     'spark/mllib/regression/common'
    autoload_without_import :ClassificationMethodBase, 'spark/mllib/classification/common'

    # Linear algebra
    autoload :Vectors,      'spark/mllib/vector'
    autoload :DenseVector,  'spark/mllib/vector'
    autoload :SparseVector, 'spark/mllib/vector'
    autoload :Matrices,     'spark/mllib/matrix'
    autoload :DenseMatrix,  'spark/mllib/matrix'
    autoload :SparseMatrix, 'spark/mllib/matrix'

    # Regression
    autoload :LabeledPoint,            'spark/mllib/regression/labeled_point'
    autoload :RegressionModel,         'spark/mllib/regression/common'
    autoload :LinearRegressionModel,   'spark/mllib/regression/linear'
    autoload :LinearRegressionWithSGD, 'spark/mllib/regression/linear'
    autoload :LassoModel,              'spark/mllib/regression/lasso'
    autoload :LassoWithSGD,            'spark/mllib/regression/lasso'
    autoload :RidgeRegressionModel,    'spark/mllib/regression/ridge'
    autoload :RidgeRegressionWithSGD,  'spark/mllib/regression/ridge'

    # Classification
    autoload :ClassificationModel,         'spark/mllib/classification/common'
    autoload :LogisticRegressionWithSGD,   'spark/mllib/classification/logistic_regression'
    autoload :LogisticRegressionWithLBFGS, 'spark/mllib/classification/logistic_regression'
    autoload :SVMModel,                    'spark/mllib/classification/svm'
    autoload :SVMWithSGD,                  'spark/mllib/classification/svm'
    autoload :NaiveBayesModel,             'spark/mllib/classification/naive_bayes'
    autoload :NaiveBayes,                  'spark/mllib/classification/naive_bayes'

    # Clustering
    autoload :KMeans,               'spark/mllib/clustering/kmeans'
    autoload :KMeansModel,          'spark/mllib/clustering/kmeans'
    autoload :GaussianMixture,      'spark/mllib/clustering/gaussian_mixture'
    autoload :GaussianMixtureModel, 'spark/mllib/clustering/gaussian_mixture'

    # Stat
    autoload :MultivariateGaussian, 'spark/mllib/stat/distribution'

    def self.prepare
      return if @prepared

      # if narray?
      #   require 'spark/mllib/narray/vector'
      #   require 'spark/mllib/narray/matrix'
      # elsif mdarray?
      #   require 'spark/mllib/mdarray/vector'
      #   require 'spark/mllib/mdarray/matrix'
      # else
      #   require 'spark/mllib/matrix/vector'
      #   require 'spark/mllib/matrix/matrix'
      # end

      require 'spark/mllib/ruby_matrix/vector_adapter'
      require 'spark/mllib/ruby_matrix/matrix_adapter'

      @prepared = true
      nil
    end

    def self.narray?
      Gem::Specification::find_all_by_name('narray').any?
    end

    def self.mdarray?
      Gem::Specification::find_all_by_name('mdarray').any?
    end
  end
end

Spark::Mllib.prepare
