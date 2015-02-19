require 'spec_helper'

def to_s_method(x)
  x.to_s
end

RSpec::describe Spark::CommandBuilder do

  context '.serialize_function' do
    let(:numbers) { Generator.numbers }
    let(:result)  { numbers.map(&:to_s) }
    let(:rdd)     { $sc.parallelize(numbers, 1) }

    it 'string' do
      expect(rdd.map('lambda{|x| x.to_s}').collect).to eql(result)
    end

    it 'symbol' do
      expect(rdd.map(:to_s).collect).to eql(result)
    end

    it 'lambda' do
      expect(rdd.map(lambda{|x| x.to_s}).collect).to eql(result)
    end

    it 'method' do
      expect(rdd.map(method(:to_s_method)).collect).to eql(result)
    end
  end

end
