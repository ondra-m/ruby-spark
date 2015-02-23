module Spark
  module Mllib
    # Linear algebra
    autoload :DenseVector,  'spark/mllib/vector'
    autoload :SparseVector, 'spark/mllib/vector'

    # Regression
    autoload :LabeledPoint,            'spark/mllib/regression'
    autoload :LinearRegressionWithSGD, 'spark/mllib/regression'

    def self.load
      return if @loaded

      if narray?
        require 'spark/mllib/narray/vector'
      elsif mdarray?
        require 'spark/mllib/mdarray/vector'
      end

      Object.const_set(:DenseVector, DenseVector)
      Object.const_set(:SparseVector, SparseVector)
      Object.const_set(:LabeledPoint, LabeledPoint)
      Object.const_set(:LinearRegressionWithSGD, LinearRegressionWithSGD)

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
