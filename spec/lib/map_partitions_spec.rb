require "spec_helper"

RSpec::describe "Spark::RDD.map_partitions" do

  context "throught parallelize" do
    let(:example_1) do
      data = (0..100).to_a
      function = lambda {|x| x.reduce(:+)}
      result = data.reduce(:+)
      Example.new(function, data, result)
    end

    let(:example_2) do
      data = (0..100).map{(rand*100).to_i}
      function = lambda {|x| x.reduce(:*)}
      result = data.reduce(:*)
      Example.new(function, data, result)
    end

    it "default parallelism" do
      result = $sc.parallelize(example_1.task).map_partitions(example_1.function).collect
      expect(result.reduce(:+)).to eq(example_1.result)

      result = $sc.parallelize(example_2.task).map_partitions(example_2.function).collect
      expect(result.reduce(:*)).to eq(example_2.result)
    end

    it "one worker" do
      result = $sc.parallelize(example_1.task, 1).map_partitions(example_1.function).collect[0]
      expect(result).to eq(example_1.result)

      result = $sc.parallelize(example_2.task, 1).map_partitions(example_2.function).collect[0]
      expect(result).to eq(example_2.result)
    end

    it "5 workers" do
      result = $sc.parallelize(example_1.task, 5).map_partitions(example_1.function).collect
      expect(result.reduce(:+)).to eq(example_1.result)

      result = $sc.parallelize(example_2.task, 5).map_partitions(example_2.function).collect
      expect(result.reduce(:*)).to eq(example_2.result)

    end
  end

  context "throught text_file" do
    let(:file) { File.join("spec", "inputs", "numbers_0_100.txt") }

    let(:example_1) do
      data = File.readlines(file).map(&:strip)
      function = lambda {|x| 
        x.map{|x| x.to_i+1}.reduce(:+)
      }
      result = function.call(data)
      Example.new(function, file, result)
    end

    let(:example_2) do
      data = File.readlines(file).map(&:strip)
      function = lambda {|x| x.map(&:to_i).reduce(:*) }
      result = function.call(data)
      Example.new(function, file, result)
    end

    it "default parallelism" do
      result = $sc.text_file(example_1.task).map_partitions(example_1.function).collect
      expect(result.reduce(:+)).to eq(example_1.result)

      result = $sc.text_file(example_2.task).map_partitions(example_2.function).collect
      expect(result.reduce(:*)).to eq(example_2.result)
    end

    it "one worker" do
      result = $sc.text_file(example_1.task, 1).map_partitions(example_1.function).collect[0]
      expect(result).to eq(example_1.result)

      result = $sc.text_file(example_2.task, 1).map_partitions(example_2.function).collect[0]
      expect(result).to eq(example_2.result)
    end

    it "9 worker" do
      result = $sc.text_file(example_1.task, 9).map_partitions(example_1.function).collect
      expect(result.reduce(:+)).to eq(example_1.result)

      result = $sc.text_file(example_2.task, 9).map_partitions(example_2.function).collect
      expect(result.reduce(:*)).to eq(example_2.result)
    end
  end

end
