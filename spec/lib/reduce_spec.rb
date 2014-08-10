require "spec_helper"

def longest_words(memo, word)
  memo.length > word.length ? memo : word
end

RSpec::shared_examples "a reducing" do |workers|
  it "with #{workers || 'default'} worker" do
    rdd2 = rdd_numbers(workers)
    rdd2 = rdd2.map(to_i)
    rdd2 = rdd2.reduce(func1)
    result = numbers.map(&:to_i).reduce(&func1)

    expect(rdd2.collect).to eql([result])

    rdd3 = rdd_numbers(workers)
    rdd3 = rdd3.map(to_i)
    rdd3 = rdd3.reduce(func2)
    result = numbers.map(&:to_i).reduce(&func2)

    expect(rdd3.collect).to eql([result])

    rdd4 = rdd_lines(workers)
    rdd4 = rdd4.flat_map(split)
    rdd4 = rdd4.reduce(:longest_words)

    result = lines.flat_map(&split).reduce(&lambda(&method(:longest_words)))

    expect(rdd4.collect).to eql([result])
  end
end

RSpec::describe "Spark::RDD.reduce" do
  let(:func1) { lambda{|sum, x| sum+x} }
  let(:func2) { lambda{|product, x| product*x} }

  let(:to_i)  { lambda{|item| item.to_i} }
  let(:split) { lambda{|item| item.split} }

  context "throught parallelize" do
    let(:numbers) { Generator.numbers }
    let(:lines)   { Generator.lines }

    def rdd_numbers(workers)
      $sc.parallelize(numbers, workers)
    end

    def rdd_lines(workers)
      $sc.parallelize(lines, workers)
    end

    it_behaves_like "a reducing", nil
    it_behaves_like "a reducing", 1
    it_behaves_like "a reducing", rand(2..10)
  end

  context "throught text_file" do
    let(:file)       { File.join("spec", "inputs", "numbers_0_100.txt") }
    let(:file_lines) { File.join("spec", "inputs", "lorem_300.txt") }

    let(:numbers) { File.readlines(file).map(&:strip) }
    let(:lines)   { File.readlines(file_lines).map(&:strip) }

    def rdd_numbers(workers)
      $sc.text_file(file, workers)
    end

    def rdd_lines(workers)
      $sc.text_file(file_lines, workers)
    end

    it_behaves_like "a reducing", nil
    it_behaves_like "a reducing", 1
    it_behaves_like "a reducing", rand(2..10)
  end
end
