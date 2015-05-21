require 'spec_helper'

RSpec.shared_examples 'a flat mapping' do |workers|
  it "with #{workers || 'default'} worker" do
    rdd2 = rdd(workers).map(func1)
    result = numbers.flat_map(&func1)

    expect(rdd2.collect).to eql(result)

    rdd3 = rdd(workers)
    rdd3 = rdd3.flat_map(func1)
    rdd3 = rdd3.flat_map(func2)
    rdd3 = rdd3.flat_map(func3)
    result = numbers.flat_map(&func1).flat_map(&func2).flat_map(&func3)

    expect(rdd3.collect).to eql(result)

    rdd4 = rdd(workers)
    rdd4 = rdd4.flat_map(func1)
    rdd4 = rdd4.flat_map(func2)
    rdd4 = rdd4.flat_map(func3)

    expect(rdd4.collect).to eql(rdd3.collect)
  end
end

RSpec.shared_examples 'a flat mapping values' do |workers|
  it "with #{workers || 'default'} worker" do
    rdd2 = rdd(workers).flat_map_values(func1)
    result = []
    hash_with_values.each do |(key, values)|
      values = func1.call(values).flatten
      values.each do |value|
        result << [key, value]
      end
    end

    expect(rdd2.collect).to eql(result)

    rdd2 = rdd(workers).flat_map_values(func2)
    result = []
    hash_with_values.each do |(key, values)|
      values = func2.call(values).flatten
      values.each do |value|
        result << [key, value]
      end
    end

    expect(rdd2.collect).to eql(result)
  end
end

RSpec.describe 'Spark::RDD' do
  let(:func1) { lambda{|x| x*2} }
  let(:func2) { lambda{|x| [x*3, 1, 1]} }
  let(:func3) { lambda{|x| [x*4, 2, 2]} }

  context 'throught parallelize' do
    context '.flat_map' do
      let(:numbers) { Generator.numbers_with_zero }

      def rdd(workers)
        $sc.parallelize(numbers, workers)
      end

      it_behaves_like 'a flat mapping', 1
      it_behaves_like 'a flat mapping', 2
      # it_behaves_like 'a flat mapping', nil
      # it_behaves_like 'a flat mapping', rand(2..10)
    end

    context '.flat_map_values' do
      let(:func1) { lambda{|x| x*2} }
      let(:func2) { lambda{|x| [x.first]} }
      let(:hash_with_values) { Generator.hash_with_values }

      def rdd(workers)
        $sc.parallelize(hash_with_values, workers)
      end

      it_behaves_like 'a flat mapping values', 1
      it_behaves_like 'a flat mapping values', 2
      # it_behaves_like 'a flat mapping values', nil
      # it_behaves_like 'a flat mapping values', rand(2..10)
    end
  end

  context 'throught text_file' do
    context '.flat_map' do
      let(:file)    { File.join('spec', 'inputs', 'numbers_0_100.txt') }
      let(:numbers) { File.readlines(file).map(&:strip) }

      def rdd(workers)
        $sc.text_file(file, workers)
      end

      it_behaves_like 'a flat mapping', 1
      it_behaves_like 'a flat mapping', 2
      # it_behaves_like 'a flat mapping', nil
      # it_behaves_like 'a flat mapping', rand(2..10)
    end
  end
end
