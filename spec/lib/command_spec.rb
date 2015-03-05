require 'spec_helper'

def to_s_method(x)
  x.to_s
end

RSpec::describe Spark::CommandBuilder do
  let(:numbers) { Generator.numbers }
  let(:rdd)     { $sc.parallelize(numbers, 1) }

  context '.serialize_function' do
    let(:result)  { numbers.map(&:to_s) }

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

  context '.bind' do
    it 'number' do
      number = rand(0..10000000)
      rdd2 = rdd.map(lambda{|x| x * number}).bind(number: number)

      expect(rdd2.collect).to eq(numbers.map{|x| x * number})
    end

    it 'open struct' do
      require 'ostruct'

      struct = OpenStruct.new
      struct.number = 3
      struct.string = '3'
      struct.array = [1, 2, 3]

      func = lambda{|item|
        item * struct.number + struct.string.to_i + struct.array[0]
      }

      rdd2 = rdd.add_library('ostruct')
      rdd2 = rdd2.map(func)
      rdd2 = rdd2.bind(struct: struct)

      expect(rdd2.collect).to eq(numbers.map(&func))
    end

    it 'different naming' do
      array = [1, 2, 3]

      rdd2 = rdd.map(lambda{|_| my_array.size})
      rdd2 = rdd2.bind(my_array: array)

      expect(rdd2.sum).to eq(numbers.size * array.size)
    end
  end

end
