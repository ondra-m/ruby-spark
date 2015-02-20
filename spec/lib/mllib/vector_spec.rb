require 'spec_helper'

describe Spark::Mllib::Vector do
  it 'dot' do
    sv = SparseVector.new(4, {1 => 1, 3 => 2})
    dv = DenseVector.new([1.0, 2.0, 3.0, 4.0])
    lst = DenseVector.new([1, 2, 3, 4])

    expect(sv.dot(dv)).to eql([10.0])
    expect(dv.dot(dv)).to eql([30.0])
    expect(lst.dot(dv)).to eql([30.0])
  end
end
