require 'spec_helper'

RSpec.describe Spark::Mllib::Vector do

  context 'parsing' do
    it 'dense vector' do
      dv  = DenseVector.new([1.0, 2.0, 3.0, 4.0, 5.0])
      dv2 = DenseVector.parse(dv.to_s)
      dv3 = Vector.parse(dv.to_s)

      expect(dv.to_s).to eq("[1.0,2.0,3.0,4.0,5.0]")
      expect(dv2.values).to eql(dv.values)
      expect(dv3.values).to eql(dv.values)
    end

    it 'sparse vector' do
      sv  = SparseVector.new(5, {1 => 3, 4 => 5})
      sv2 = SparseVector.parse(sv.to_s)
      sv3 = Vector.parse(sv.to_s)

      expect(sv.to_s).to eq("(5,[1,4],[3.0,5.0])")
      expect(sv2.size).to eq(sv.size)
      expect(sv2.indices).to eq(sv.indices)
      expect(sv2.values).to eq(sv.values)
      expect(sv3.size).to eq(sv.size)
      expect(sv3.indices).to eq(sv.indices)
      expect(sv3.values).to eq(sv.values)
    end
  end

  it 'dot' do
    sv = SparseVector.new(4, {1 => 1, 3 => 2})
    dv = DenseVector.new([1.0, 2.0, 3.0, 4.0])
    lst = DenseVector.new([1, 2, 3, 4])

    expect(sv.dot(dv)).to eql(10.0)
    expect(dv.dot(dv)).to eql(30.0)
    expect(lst.dot(dv)).to eql(30.0)
  end

  it 'squared distance' do
    sv = SparseVector.new(4, {1 => 1, 3 => 2})
    dv = DenseVector.new([1.0, 2.0, 3.0, 4.0])
    lst = DenseVector.new([4, 3, 2, 1])

    expect(sv.squared_distance(dv)).to eql(15.0)
    expect(sv.squared_distance(lst)).to eql(25.0)
    expect(dv.squared_distance(lst)).to eql(20.0)
    expect(dv.squared_distance(sv)).to eql(15.0)
    expect(lst.squared_distance(sv)).to eql(25.0)
    expect(lst.squared_distance(dv)).to eql(20.0)
    expect(sv.squared_distance(sv)).to eql(0.0)
    expect(dv.squared_distance(dv)).to eql(0.0)
    expect(lst.squared_distance(lst)).to eql(0.0)
  end

  it 'sparse vector indexing' do
    sv1 = SparseVector.new(4, {1 => 1, 3 => 2})
    sv2 = SparseVector.new(4, [[1, 3], [1, 2]])

    expect(sv1[0]).to eq(0.0)
    expect(sv1[3]).to eq(2.0)
    expect(sv1[1]).to eq(1.0)
    expect(sv1[2]).to eq(0.0)
    expect(sv1[-1]).to eq(2)
    expect(sv1[-2]).to eq(0)
    expect(sv1[-4]).to eq(0)

    expect(sv2[0]).to eq(0.0)
    expect(sv2[3]).to eq(2.0)
    expect(sv2[1]).to eq(1.0)
    expect(sv2[2]).to eq(0.0)
    expect(sv2[-1]).to eq(2)
    expect(sv2[-2]).to eq(0)
    expect(sv2[-4]).to eq(0)
  end
end
