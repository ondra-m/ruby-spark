require "spec_helper"

def func4(item)
  item.start_with?("a") && item[1].to_s.ord > 106 && item.size > 3
end

RSpec::shared_examples "a filtering" do |workers|
  context "with #{workers || 'default'} worker" do
    it "when numbers" do
      rdd2 = rdd_numbers(workers)
      rdd2 = rdd2.filter(func1)
      result = numbers.select(&func1)

      expect(rdd2.collect).to eql(result)

      rdd3 = rdd_numbers(workers)
      rdd3 = rdd3.filter(func1)
      rdd3 = rdd3.filter(func2)

      expect(rdd3.collect).to eql([])
    end

    it "when chars" do
      rdd2 = rdd_chars(workers)
      rdd2 = rdd2.filter(func3)
      result = chars.select{|x| func3.call(x)}  

      expect(rdd2.collect).to eql(result)

      rdd3 = rdd_chars(workers)
      rdd3 = rdd3.filter(:func4)
      result = chars.select{|x| func4(x)}

      expect(rdd3.collect).to eql(result)
    end
  end
end

RSpec::describe "Spark::RDD.filter" do
  let(:func1) { lambda{|x| x.to_i.even?} }
  let(:func2) { lambda{|x| x.to_i.odd?} }
  let(:func3) { lambda{|x| x.to_s.start_with?("b")} }

  context "throught parallelize" do
    let(:numbers) { 0..1000 }
    let(:chars) do
      Array.new(10){ 
        Array.new(rand(1..10)){(97+rand(26)).chr}.join
      }
    end

    def rdd_numbers(workers)
      $sc.parallelize(numbers, workers)
    end

    def rdd_chars(workers)
      $sc.parallelize(chars, workers)
    end

    it_behaves_like "a filtering", nil
    it_behaves_like "a filtering", 1
    it_behaves_like "a filtering", rand(1..10)
  end

  context "throught text_file" do
    let(:file_numbers) { File.join("spec", "inputs", "numbers_0_100.txt") }
    let(:file_chars)   { File.join("spec", "inputs", "lorem_300.txt") }

    let(:numbers) { File.readlines(file_numbers).map(&:strip) }
    let(:chars)   { File.readlines(file_chars).map(&:strip) }

    def rdd_numbers(workers)
      $sc.text_file(file_numbers, workers)
    end

    def rdd_chars(workers)
      $sc.text_file(file_chars, workers)
    end

    it_behaves_like "a filtering", nil
    it_behaves_like "a filtering", 1
    it_behaves_like "a filtering", rand(1..10)
  end
end
