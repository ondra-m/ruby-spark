require 'spec_helper'

def flat_map(line)
  line.split
end

def map(item)
  [item, 1]
end

def reduce(x,y)
  x+y
end

RSpec.shared_examples 'a words counting' do |workers|
  context "with #{workers || 'default'} worker" do
    let(:result) do
      keyyed = lines.flat_map{|x| x.split}.map{|x| [x,1]}
      result = keyyed.reduce({}){|memo, item|
        key   = item[0]
        value = item[1]

        memo[key] ||= 0
        memo[key] += value
        memo
      }
      result
    end

    it 'when lambda' do
      rdd2 = rdd(workers)
      rdd2 = rdd2.flat_map(lambda{|line| line.split})
      rdd2 = rdd2.map(lambda{|word| [word, 1]})
      rdd2 = rdd2.reduce_by_key(lambda{|x,y| x+y})

      expect(rdd2.collect_as_hash).to eql(result)
    end

    it 'when method' do
      rdd2 = rdd(workers)
      rdd2 = rdd2.flat_map(method(:flat_map))
      rdd2 = rdd2.map(method(:map))
      rdd2 = rdd2.reduce_by_key(method(:reduce))

      expect(rdd2.collect_as_hash).to eql(result)
    end

    it 'keys, values' do
      rdd2 = rdd(workers)
      rdd2 = rdd2.flat_map(method(:flat_map))
      rdd2 = rdd2.map(method(:map))
      rdd2 = rdd2.reduce_by_key(method(:reduce))

      expect(rdd2.keys.collect.sort).to eql(result.keys.sort)
      expect { rdd2.values.collect.reduce(:+) }.to_not raise_error
    end
  end
end

RSpec.describe 'Spark::RDD' do
  context '.reduce_by_key' do
    context 'throught parallelize' do
      let(:lines) { Generator.lines }

      def rdd(workers)
        $sc.parallelize(lines, workers)
      end

      it_behaves_like 'a words counting', 2
      # it_behaves_like 'a words counting', nil
      # it_behaves_like 'a words counting', rand(2..10)
    end

    context 'throught text_file' do
      let(:file)  { File.join('spec', 'inputs', 'lorem_300.txt') }
      let(:lines) { File.readlines(file).map(&:strip) }

      def rdd(workers)
        $sc.text_file(file, workers)
      end

      it_behaves_like 'a words counting', 2
      # it_behaves_like 'a words counting', nil
      # it_behaves_like 'a words counting', rand(2..10)
    end
  end

  context '.fold_by_key' do
    let(:numbers)    { Generator.numbers }
    let(:zero_value) { 0 }
    let(:rdd)        { $sc.parallelize(numbers) }
    let(:map)        { lambda{|x| [x, 1]} }
    let(:add)        { lambda{|x,y| x+y} }

    let(:result) do
      _result = {}
      numbers.map(&map).each do |key, value|
        _result[key] ||= zero_value
        _result[key] = add.call(_result[key], value)
      end
      _result
    end

    def fold_by_key(num_partitions=nil)
      rdd.map(map).fold_by_key(zero_value, add, num_partitions).collect_as_hash
    end

    it 'default num_partitions' do
      expect(fold_by_key).to eq(result)
    end

    it 'default num_partitions' do
      expect(
        fold_by_key rand(1..10)
      ).to eq(result)
    end
  end
end
