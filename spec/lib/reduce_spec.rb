require "spec_helper"

def flat_map(line)
  line.split
end

def map(item)
  [item, 1]
end

def reduce(x,y)
  x+y
end

RSpec::shared_examples "a words counting" do |workers|
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

    it "when lambda" do
      rdd2 = rdd(workers)
      rdd2 = rdd2.flat_map(lambda{|line| line.split})
      rdd2 = rdd2.map(lambda{|word| [word, 1]})
      rdd2 = rdd2.reduce_by_key(lambda{|x,y| x+y})

      expect(rdd2.collect_as_hash).to eql(result)
    end

    it "when method" do
      rdd2 = rdd(workers)
      rdd2 = rdd2.flat_map(:flat_map)
      rdd2 = rdd2.map(:map)
      rdd2 = rdd2.reduce_by_key(:reduce)

      expect(rdd2.collect_as_hash).to eql(result)
    end
  end
end

RSpec::describe "Spark::RDD reducing" do
  context "throught parallelize" do
    let(:lines) do
      Array.new(1000){ 
        Array.new(rand(50..100)){
          # (97+rand(26)).chr + (" " * (rand(10) == 0 ? 1 : 0))
          (97+rand(3)).chr + (" " * (rand(10) == 0 ? 1 : 0))
        }.join
      }
    end

    def rdd(workers)
      $sc.parallelize(lines, workers)
    end

    it_behaves_like "a words counting", nil
    it_behaves_like "a words counting", 1
    it_behaves_like "a words counting", rand(1..10)
  end

  context "throught text_file" do
    let(:file) { File.join("spec", "inputs", "lorem_300.txt") }
    let(:lines) { File.readlines(file).map(&:strip) }

    def rdd(workers)
      $sc.text_file(file, workers)
    end

    it_behaves_like "a words counting", nil
    it_behaves_like "a words counting", 1
    it_behaves_like "a words counting", rand(1..10)
  end
end
