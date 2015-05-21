require 'spec_helper'

RSpec.describe Spark::RDD do

  let(:mapping) { lambda{|x| [x, 1]} }
  let(:numbers) { Generator.numbers }

  it '.collect_as_hash' do
    rdd = $sc.parallelize(numbers)
    rdd = rdd.map(mapping)

    expect(rdd.collect_as_hash).to eql(Hash[numbers.map(&mapping)])
  end

  context '.take' do
    let(:size)    { 1000 }
    let(:numbers) { Generator.numbers(size) }
    let(:rdd)     { $sc.parallelize(numbers) }

    it 'nothing' do
      expect(rdd.take(0)).to eql([])
    end

    it 'first' do
      expect(rdd.first).to eql(numbers.first)
    end

    it 'less than limit' do
      _size = size / 2
      expect(rdd.take(_size)).to eql(numbers.take(_size))
    end

    it 'all' do
      expect(rdd.take(size)).to eql(numbers)
    end

    it 'more than limit' do
      expect(rdd.take(size*2)).to eql(numbers)
    end
  end

end
