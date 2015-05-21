require 'spec_helper'

RSpec.shared_examples 'a mapping' do |workers|
  it "with #{workers || 'default'} worker" do
    rdd2 = rdd(workers).map(func1)
    result = numbers.map(&func1)

    expect(rdd2.collect).to eql(result)

    rdd3 = rdd(workers)
    rdd3 = rdd3.map(func1)
    rdd3 = rdd3.map(func2)
    rdd3 = rdd3.map(func3)
    result = numbers.map(&func1).map(&func2).map(&func3)

    expect(rdd3.collect).to eql(result)

    rdd4 = rdd(workers)
    rdd4 = rdd4.map(func3)
    rdd4 = rdd4.map(func2)
    rdd4 = rdd4.map(func1)

    expect(rdd4.collect).to eql(rdd3.collect)
  end
end

RSpec.shared_examples 'a mapping values' do |workers|
  it "with #{workers || 'default'} worker" do
    rdd2 = rdd(workers).map_values(func1)
    result = hash.map{|key, value| [key, func1.call(value)]}

    expect(rdd2.collect).to eql(result)

    rdd3 = rdd(workers)
    rdd3 = rdd3.map_values(func1)
    rdd3 = rdd3.map_values(func2)
    rdd3 = rdd3.map_values(func3)
    result = hash.map{|key, value| [key, func1.call(value)]}
                 .map{|key, value| [key, func2.call(value)]}
                 .map{|key, value| [key, func3.call(value)]}

    expect(rdd3.collect).to eql(result)
  end
end

RSpec.describe 'Spark::RDD' do
  let(:func1) { lambda{|x| x*2} }
  let(:func2) { lambda{|x| x*3} }
  let(:func3) { lambda{|x| x*4} }

  context 'throught parallelize' do
    context '.map' do
      let(:numbers) { Generator.numbers }

      def rdd(workers)
        $sc.parallelize(numbers, workers)
      end

      it_behaves_like 'a mapping', 1
      it_behaves_like 'a mapping', 2
      # it_behaves_like 'a mapping', nil
      # it_behaves_like 'a mapping', rand(2..10)
    end

    context '.map_values' do
      let!(:hash) { Generator.hash }

      def rdd(workers)
        $sc.parallelize(hash, workers)
      end

      it_behaves_like 'a mapping values', 1
      it_behaves_like 'a mapping values', 2
      # it_behaves_like 'a mapping values', nil
      # it_behaves_like 'a mapping values', rand(2..10)
    end
  end

  context 'throught text_file' do
    context '.map' do
      let(:file) { File.join('spec', 'inputs', 'numbers_0_100.txt') }
      let(:numbers) { File.readlines(file).map(&:strip) }

      def rdd(workers)
        $sc.text_file(file, workers)
      end

      it_behaves_like 'a mapping', 1
      it_behaves_like 'a mapping', 2
      # it_behaves_like 'a mapping', nil
      # it_behaves_like 'a mapping', rand(2..10)
    end
  end
end
