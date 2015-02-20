module Spark
  module Mllib
    # Linear algebra
    autoload :DenseVector,  'spark/mllib/vector'
    autoload :SparseVector, 'spark/mllib/vector'

    def self.load
      Object.const_set(:DenseVector, DenseVector)
      Object.const_set(:SparseVector, SparseVector)
      nil
    end
  end
end
