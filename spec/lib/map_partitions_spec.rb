require 'spec_helper'

def func3(x)
  x.map(&:to_i).reduce(:+)
end

def func4_with_index(data, index)
  [{
    index => data.map(&:to_i).reduce(:*)
  }]
end

RSpec.shared_examples 'a map partitions' do |workers|
  context "with #{workers || 'default'} worker" do
    it 'without index' do
      rdd2 = rdd(workers).map_partitions(func1)
      result = func1.call(numbers)

      expect(func1.call(rdd2.collect)).to eql(result)

      rdd3 = rdd(workers)
      rdd3 = rdd3.map_partitions(func1)
      rdd3 = rdd3.map_partitions(func2)
      rdd3 = rdd3.map_partitions(method(:func3))
      result = func3(func2.call(func1.call(numbers)))

      # Not same number of workers
      expect(rdd3.collect.size).to be >= 1

      rdd4 = rdd(workers)
      rdd4 = rdd4.map_partitions(func1)
      rdd4 = rdd4.map_partitions(func2)
      rdd4 = rdd4.map_partitions(method(:func3))

      expect(rdd4.collect).to eql(rdd3.collect)
    end

    it 'with index' do
      rdd2 = rdd(workers).map_partitions_with_index(method(:func4_with_index))
      result = rdd2.collect

      expect(result).to be_a(Array)

      result.each do |x|
        expect(x).to be_a(Hash)
      end

      # Multiply by 0
      # Some values are 0 because of batched serialization
      expect(result.map(&:values).flatten.compact.uniq.first).to eql(0)
    end
  end
end

RSpec::describe 'Spark::RDD.map_partitions(_with_index)' do
  let(:func1) { lambda{|x| x.map(&:to_i)} }
  let(:func2) {
    lambda{|x|
      x.map{|y| y*2}
    }
  }

  context 'throught parallelize' do
    let(:numbers) { 0..1000 }

    def rdd(workers)
      $sc.parallelize(numbers, workers)
    end

    it_behaves_like 'a map partitions', 1
    it_behaves_like 'a map partitions', 2
    # it_behaves_like 'a map partitions', nil
    # it_behaves_like 'a map partitions', rand(2..10)
  end

  context 'throught text_file' do
    let(:file)    { File.join('spec', 'inputs', 'numbers_0_100.txt') }
    let(:numbers) { File.readlines(file).map(&:strip) }

    def rdd(workers)
      $sc.text_file(file, workers)
    end

    it_behaves_like 'a map partitions', 1
    it_behaves_like 'a map partitions', 2
    # it_behaves_like 'a map partitions', nil
    # it_behaves_like 'a map partitions', rand(2..10)
  end
end
