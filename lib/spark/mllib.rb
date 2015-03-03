module Spark
  module Mllib
    # Linear algebra
    autoload :Vector,       'spark/mllib/vector'
    autoload :DenseVector,  'spark/mllib/vector'
    autoload :SparseVector, 'spark/mllib/vector'
    autoload :Matrix,       'spark/mllib/matrix'
    autoload :DenseMatrix,  'spark/mllib/matrix'
    autoload :SparseMatrix, 'spark/mllib/matrix'

    # Regression
    autoload :LabeledPoint,            'spark/mllib/regression/labeled_point'
    autoload :RegressionModel,         'spark/mllib/regression/common'
    autoload :RegressionMethodBase,    'spark/mllib/regression/common'
    autoload :LinearRegressionModel,   'spark/mllib/regression/linear'
    autoload :LinearRegressionWithSGD, 'spark/mllib/regression/linear'
    autoload :LassoModel,              'spark/mllib/regression/lasso'
    autoload :LassoWithSGD,            'spark/mllib/regression/lasso'
    autoload :RidgeRegressionModel,    'spark/mllib/regression/ridge'
    autoload :RidgeRegressionWithSGD,  'spark/mllib/regression/ridge'

    # Classification
    autoload :ClassificationModel,         'spark/mllib/classification/common'
    autoload :ClassificationMethodBase,    'spark/mllib/classification/common'
    autoload :LogisticRegressionWithSGD,   'spark/mllib/classification/logistic_regression'
    autoload :LogisticRegressionWithLBFGS, 'spark/mllib/classification/logistic_regression'
    autoload :SVMModel,                    'spark/mllib/classification/svm'
    autoload :SVMWithSGD,                  'spark/mllib/classification/svm'
    autoload :NaiveBayesModel,             'spark/mllib/classification/naive_bayes'
    autoload :NaiveBayes,                  'spark/mllib/classification/naive_bayes'

    def self.prepare
      return if @prepared

      if narray?
        require 'spark/mllib/narray/vector'
        require 'spark/mllib/narray/matrix'
      elsif mdarray?
        require 'spark/mllib/mdarray/vector'
        require 'spark/mllib/mdarray/matrix'
      else
        require 'spark/mllib/matrix/vector'
        require 'spark/mllib/matrix/matrix'
      end

      @prepared = true
      nil
    end

    def self.load
      return if @loaded

      prepare

      Object.const_set(:Vector, Vector)
      Object.const_set(:DenseVector, DenseVector)
      Object.const_set(:SparseVector, SparseVector)
      Object.const_set(:Matrix, Matrix)
      Object.const_set(:DenseMatrix, DenseMatrix)
      Object.const_set(:SparseMatrix, SparseMatrix)
      Object.const_set(:LabeledPoint, LabeledPoint)
      Object.const_set(:RegressionModel, RegressionModel)
      Object.const_set(:LinearRegressionModel, LinearRegressionModel)
      Object.const_set(:LinearRegressionWithSGD, LinearRegressionWithSGD)
      Object.const_set(:LassoModel, LassoModel)
      Object.const_set(:LassoWithSGD, LassoWithSGD)
      Object.const_set(:RidgeRegressionModel, RidgeRegressionModel)
      Object.const_set(:RidgeRegressionWithSGD, RidgeRegressionWithSGD)
      Object.const_set(:ClassificationModel, ClassificationModel)
      Object.const_set(:ClassificationMethodBase, ClassificationMethodBase)
      Object.const_set(:LogisticRegressionWithSGD, LogisticRegressionWithSGD)
      Object.const_set(:LogisticRegressionWithLBFGS, LogisticRegressionWithLBFGS)
      Object.const_set(:SVMModel, SVMModel)
      Object.const_set(:SVMWithSGD, SVMWithSGD)
      Object.const_set(:NaiveBayesModel, NaiveBayesModel)
      Object.const_set(:NaiveBayes, NaiveBayes)

      @loaded = true
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
