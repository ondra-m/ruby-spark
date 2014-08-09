require "spec_helper"

RSpec.describe "Spark::RDD.flat_map" do

  context "throught parallelize" do
    let(:example_1) do
      func = lambda {|x| [x*2, x*3, x*4]}

      e = Example.new
      e.data      = 0..100
      e.function << [:flat_map, func] 
      e.result    = e.data.flat_map(&func)
      e
    end

    let!(:example_2) do
      func = lambda {|x| [x.upcase, x]}

      e = Example.new
      e.data      = (0..100).map{(97+rand(26)).chr}
      e.function << [:flat_map, func]
      e.result    = e.data.flat_map(&func)
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

    it "5 worker" do
      example_1.workers(5).run
      example_2.workers(5).run
    end
  end

  context "throught text_file" do
    let(:file) { File.join("spec", "inputs", "numbers_0_100.txt") }
    let(:data) { File.readlines(file).map(&:strip) }

    let(:example_1) do
      func = lambda {|x| [x.to_i*2, x+'a']}

      e = Example.new
      e.file      = file
      e.function << [:flat_map, func] 
      e.result    = data.flat_map(&func)
      e
    end

    let(:example_2) do
      func = lambda {|x| [x, 'a', 'b', 'c']}

      e = Example.new
      e.file      = file
      e.function << [:flat_map, func] 
      e.result    = data.flat_map(&func)
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

    it "7 worker" do
      example_1.workers(7).run
      example_2.workers(7).run
    end
  end

end
