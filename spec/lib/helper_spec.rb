require 'spec_helper'

RSpec.configure do |c|
  c.include Spark::Helper::Parser
  c.include Spark::Helper::Statistic
end

RSpec.describe Spark::Helper do

  it 'memory size' do
    expect(to_memory_size('512mb')).to eql(524288.0)
    expect(to_memory_size('1586 mb')).to eql(1624064.0)
    expect(to_memory_size('3 MB')).to eql(3072.0)
    expect(to_memory_size('9gb')).to eql(9437184.0)
    expect(to_memory_size('9gb', 'mb')).to eql(9216.0)
    expect(to_memory_size('9mb', 'gb')).to eql(0.01)
    expect(to_memory_size('6652548796kb', 'mb')).to eql(6496629.68)
  end

  context 'statistic' do
    it 'compute_fraction' do
      expect(compute_fraction(1, 1000, true)).to be_within(0.001).of(0.013)
      expect(compute_fraction(2, 1000, true)).to be_within(0.001).of(0.018)
      expect(compute_fraction(3, 1000, true)).to be_within(0.001).of(0.023)
      expect(compute_fraction(4, 1000, true)).to be_within(0.001).of(0.028)
      expect(compute_fraction(5, 1000, true)).to be_within(0.001).of(0.031)

      expect(compute_fraction(1, 1000, false)).to be_within(0.001).of(0.0249)
      expect(compute_fraction(2, 1000, false)).to be_within(0.001).of(0.0268)
      expect(compute_fraction(3, 1000, false)).to be_within(0.001).of(0.0287)
      expect(compute_fraction(4, 1000, false)).to be_within(0.001).of(0.0305)
      expect(compute_fraction(5, 1000, false)).to be_within(0.001).of(0.0322)
    end

    it 'bisect_right' do
      data = [10, 20, 30, 40, 50, 60, 70, 80, 90]

      expect(bisect_right(data, 0)).to eq(0)
      expect(bisect_right(data, 1)).to eq(0)
      expect(bisect_right(data, 1, 2)).to eq(2)
      expect(bisect_right(data, 1, 3)).to eq(3)
      expect(bisect_right(data, 1, 4)).to eq(4)
      expect(bisect_right(data, 9)).to eq(0)
      expect(bisect_right(data, 10)).to eq(1)
      expect(bisect_right(data, 40)).to eq(4)
      expect(bisect_right(data, 42)).to eq(4)
      expect(bisect_right(data, 72)).to eq(7)
      expect(bisect_right(data, 80, 4)).to eq(8)
      expect(bisect_right(data, 80, 5)).to eq(8)
      expect(bisect_right(data, 80, 8)).to eq(8)
      expect(bisect_right(data, 80, 9)).to eq(9)
      expect(bisect_right(data, 200)).to eq(9)
    end

    it 'determine_bounds' do
      data = [10, 20, 30, 40, 50, 60, 70, 80, 90]

      expect(determine_bounds(data, 0)).to eq([])
      expect(determine_bounds(data, 1)).to eq([])
      expect(determine_bounds(data, 2)).to eq([50])
      expect(determine_bounds(data, 3)).to eq([40, 70])
      expect(determine_bounds(data, 4)).to eq([30, 50, 70])
      expect(determine_bounds(data, 20)).to eq(data)
    end
  end

end
