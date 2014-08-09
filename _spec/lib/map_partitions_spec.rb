require "spec_helper"

RSpec::describe "Spark::RDD.map_partitions" do

  context "throught parallelize" do
    let(:example_1) do
      func = lambda {|x| x.reduce(:+)}

      e = Example.new
      e.data      = (0..100).to_a
      e.function << [:map_partitions, func]
      e.function << [:coalesce, 1]
      e.function << [:map_partitions, func]
      e.result    = [func.call(e.data)]
      e
    end

    let(:example_2) do
      func = lambda {|x| x.reduce(:*)}

      e = Example.new
      e.data      = (0..100).map{(rand*100).to_i}
      e.function << [:map_partitions, func]
      e.function << [:coalesce, 1]
      e.function << [:map_partitions, func]
      e.result    = [func.call(e.data)]
      e
    end

    it "default parallelism" do
      example_1.run
      example_2.run
    end

    it "one worker" do
      example_1.workers(1).run
      example_2.workers(1).run
    end

    it "5 workers" do
      example_1.workers(5).run
      example_2.workers(5).run
    end
  end

  context "throught text_file" do
    let(:file) { File.join("spec", "inputs", "numbers_1_100.txt") }
    let(:data) { File.readlines(file).map(&:strip) }

    let(:example_1) do
      func = lambda {|x| x.map(&:to_i).reduce(:+)}

      e = Example.new
      e.file      = file
      e.function << [:map_partitions, func]
      e.function << [:coalesce, 1]
      e.function << [:map_partitions, func]
      e.result    = [func.call(data)]
      e
    end

    let(:example_2) do
      func = lambda {|x| x.map(&:to_i).reduce(:*)}

      e = Example.new
      e.file      = file
      e.function << [:map_partitions, func]
      e.function << [:coalesce, 1]
      e.function << [:map_partitions, func]
      e.result    = [func.call(data)]
      e
    end

    it "default parallelism" do
      example_1.run
      example_2.run
    end

    it "one worker" do
      example_1.workers(1).run
      example_2.workers(1).run
    end

    it "9 workers" do
      example_1.workers(9).run
      example_2.workers(9).run
    end
  end

end
