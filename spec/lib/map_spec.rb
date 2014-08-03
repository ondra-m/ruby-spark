require "spec_helper"

RSpec::describe "Spark::RDD.map" do

  context "throught parallelize" do
    let(:example_1) do
      data = 0..100
      function = lambda {|x| x*2}
      result = data.map{|x| function.call(x)}
      Example.new(function, data, result)
    end

    let(:example_2) do
      data = (0..100).map{(97+rand(26)).chr}
      function = lambda {|x| x.upcase}
      result = data.map{|x| function.call(x)}
      Example.new(function, data, result)
    end

    it "default parallelism" do
      result = $sc.parallelize(example_1.task).map(example_1.function).collect
      expect(result).to eq(example_1.result)

      result = $sc.parallelize(example_2.task).map(example_2.function).collect
      expect(result).to eq(example_2.result)
    end

    it "one worker" do
      result = $sc.parallelize(example_1.task, 1).map(example_1.function).collect
      expect(result).to eq(example_1.result)

      result = $sc.parallelize(example_2.task, 1).map(example_2.function).collect
      expect(result).to eq(example_2.result)
    end

    it "5 worker" do
      result = $sc.parallelize(example_1.task, 5).map(example_1.function).collect
      expect(result).to eq(example_1.result)

      result = $sc.parallelize(example_2.task, 5).map(example_2.function).collect
      expect(result).to eq(example_2.result)
    end
  end

  context "throught text_file" do
    let(:file) { File.join("spec", "inputs", "numbers_0_100.txt") }

    let(:example_1) do
      data = File.readlines(file)
      function = lambda {|x| x.to_i*2}
      result = data.map{|x| function.call(x)}
      Example.new(function, file, result)
    end

    let(:example_2) do
      data = File.readlines(file).map(&:strip)
      function = lambda {|x| x+"a"+"b"+"c"}
      result = data.map{|x| function.call(x)}
      Example.new(function, file, result)
    end

    it "default parallelism" do
      result = $sc.text_file(example_1.task).map(example_1.function).collect
      expect(result).to eq(example_1.result)

      result = $sc.text_file(example_2.task).map(example_2.function).collect
      expect(result).to eq(example_2.result)
    end

    it "one worker" do
      result = $sc.text_file(example_1.task, 1).map(example_1.function).collect
      expect(result).to eq(example_1.result)

      result = $sc.text_file(example_2.task, 1).map(example_2.function).collect
      expect(result).to eq(example_2.result)
    end

    it "3 worker" do
      result = $sc.text_file(example_1.task, 3).map(example_1.function).collect
      expect(result).to eq(example_1.result)

      result = $sc.text_file(example_2.task, 3).map(example_2.function).collect
      expect(result).to eq(example_2.result)
    end
  end

end
