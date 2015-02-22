module Spark
  module Mllib
    # Linear algebra
    autoload :DenseVector,  'spark/mllib/vector'
    autoload :SparseVector, 'spark/mllib/vector'

    # Regression
    autoload :LabeledPoint,            'spark/mllib/regression'
    autoload :LinearRegressionWithSGD, 'spark/mllib/regression'

    def self.load
      Object.const_set(:DenseVector, DenseVector)
      Object.const_set(:SparseVector, SparseVector)
      Object.const_set(:LabeledPoint, LabeledPoint)
      Object.const_set(:LinearRegressionWithSGD, LinearRegressionWithSGD)
      nil
    end
  end
end
