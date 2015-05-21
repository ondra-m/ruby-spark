require 'spec_helper'

def longest_words(memo, word)
  memo.length > word.length ? memo : word
end

RSpec.shared_examples 'a reducing' do |workers|
  context "with #{workers || 'default'} worker" do
    it '.reduce' do
      rdd2 = rdd_numbers(workers)
      rdd2 = rdd2.map(to_i)
      rdd2 = rdd2.reduce(func1)
      result = numbers.map(&:to_i).reduce(&func1)

      expect(rdd2).to eql(result)

      rdd3 = rdd_numbers(workers)
      rdd3 = rdd3.map(to_i)
      rdd3 = rdd3.reduce(func2)
      result = numbers.map(&:to_i).reduce(&func2)

      expect(rdd3).to eql(result)

      rdd4 = rdd_lines(workers)
      rdd4 = rdd4.flat_map(split)
      rdd4 = rdd4.reduce(method(:longest_words))

      result = lines.flat_map(&split).reduce(&lambda(&method(:longest_words)))

      expect(rdd4).to eql(result)
    end

    it '.fold' do
      rdd2 = rdd_numbers(workers)
      rdd2 = rdd2.map(to_i)
      rdd_result = rdd2.fold(1, func1)

      # all workers add 1 + last reducing phase
      result = numbers.map(&:to_i).reduce(&func1) + rdd2.partitions_size + 1

      expect(rdd_result).to eql(result)
    end

    it '.aggregate' do
      rdd2 = rdd_numbers(workers)
      rdd2 = rdd2.map(to_i)

      # Sum of items + their count
      seq = lambda{|x,y| [x[0] + y, x[1] + 1]}
      com = lambda{|x,y| [x[0] + y[0], x[1] + y[1]]}
      rdd_result = rdd2.aggregate([0,0], seq, com)

      result = [numbers.reduce(:+), numbers.size]

      expect(rdd_result).to eql(result)
    end

    it '.max' do
      rdd2 = rdd_numbers(workers)
      rdd2 = rdd2.map(to_i)

      expect(rdd2.max).to eql(numbers.map(&:to_i).max)
    end

    it '.min' do
      rdd2 = rdd_numbers(workers)
      rdd2 = rdd2.map(to_i)

      expect(rdd2.min).to eql(numbers.map(&:to_i).min)
    end

    it '.sum' do
      rdd2 = rdd_numbers(workers)
      rdd2 = rdd2.map(to_i)

      expect(rdd2.sum).to eql(numbers.map(&:to_i).reduce(:+))
    end

    it '.count' do
      rdd2 = rdd_numbers(workers)
      rdd2 = rdd2.map(to_i)

      expect(rdd2.count).to eql(numbers.size)
    end
  end
end

RSpec.describe 'Spark::RDD' do
  let(:func1) { lambda{|sum, x| sum+x} }
  let(:func2) { lambda{|product, x| product*x} }

  let(:to_i)  { lambda{|item| item.to_i} }
  let(:split) { lambda{|item| item.split} }

  context 'throught parallelize' do
    let(:numbers) { Generator.numbers }
    let(:lines)   { Generator.lines }

    def rdd_numbers(workers)
      $sc.parallelize(numbers, workers)
    end

    def rdd_lines(workers)
      $sc.parallelize(lines, workers)
    end

    it_behaves_like 'a reducing', 1
    it_behaves_like 'a reducing', 2
    # it_behaves_like 'a reducing', nil
    # it_behaves_like 'a reducing', rand(2..10)
  end

  context 'throught text_file' do
    let(:file)       { File.join('spec', 'inputs', 'numbers_0_100.txt') }
    let(:file_lines) { File.join('spec', 'inputs', 'lorem_300.txt') }

    let(:numbers) { File.readlines(file).map(&:strip).map(&:to_i) }
    let(:lines)   { File.readlines(file_lines).map(&:strip) }

    def rdd_numbers(workers)
      $sc.text_file(file, workers)
    end

    def rdd_lines(workers)
      $sc.text_file(file_lines, workers)
    end

    it_behaves_like 'a reducing', 1
    it_behaves_like 'a reducing', 2
    # it_behaves_like 'a reducing', nil
    # it_behaves_like 'a reducing', rand(2..10)
  end
end
