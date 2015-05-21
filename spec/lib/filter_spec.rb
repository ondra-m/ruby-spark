require 'spec_helper'

def func4(item)
  item.start_with?('a') && item.size > 3 && item[1].to_s.ord > 106
end

RSpec.shared_examples 'a filtering' do |workers|
  context "with #{workers || 'default'} worker" do
    it 'when numbers' do
      rdd2 = rdd_numbers(workers)
      rdd2 = rdd2.filter(func1)
      result = numbers.select(&func1)

      expect(rdd2.collect).to eql(result)

      rdd3 = rdd_numbers(workers)
      rdd3 = rdd3.filter(func1)
      rdd3 = rdd3.filter(func2)

      expect(rdd3.collect).to eql([])
    end

    it 'when words' do
      rdd2 = rdd_words(workers)
      rdd2 = rdd2.filter(func3)
      result = words.select{|x| func3.call(x)}

      expect(rdd2.collect).to eql(result)

      rdd3 = rdd_words(workers)
      rdd3 = rdd3.filter(method(:func4))
      result = words.select{|x| func4(x)}

      expect(rdd3.collect).to eql(result)
    end
  end
end

RSpec.describe 'Spark::RDD.filter' do
  let(:func1) { lambda{|x| x.to_i.even?} }
  let(:func2) { lambda{|x| x.to_i.odd?} }
  let(:func3) { lambda{|x| x.to_s.start_with?('b')} }

  context 'throught parallelize' do
    let(:numbers) { Generator.numbers_with_zero }
    let(:words)   { Generator.words }

    def rdd_numbers(workers)
      $sc.parallelize(numbers, workers)
    end

    def rdd_words(workers)
      $sc.parallelize(words, workers)
    end

    it_behaves_like 'a filtering', 2
    # it_behaves_like 'a filtering', nil
    # it_behaves_like 'a filtering', rand(2..10)
  end

  context 'throught text_file' do
    let(:file_numbers) { File.join('spec', 'inputs', 'numbers_0_100.txt') }
    let(:file_words)   { File.join('spec', 'inputs', 'lorem_300.txt') }

    let(:numbers) { File.readlines(file_numbers).map(&:strip) }
    let(:words)   { File.readlines(file_words).map(&:strip) }

    def rdd_numbers(workers)
      $sc.text_file(file_numbers, workers)
    end

    def rdd_words(workers)
      $sc.text_file(file_words, workers)
    end

    it_behaves_like 'a filtering', 2
    # it_behaves_like 'a filtering', nil
    # it_behaves_like 'a filtering', rand(2..10)
  end
end
