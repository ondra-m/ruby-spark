require "spec_helper"

RSpec::describe "Spark::RDD.map" do

  context "throught parallelize" do
    let(:example_1) do
      func = lambda {|x| x*2}

      e = Example.new
      e.data      = 0..100
      e.function << [:map, func]
      e.result    = e.data.map(&func)
      e
    end

    let(:example_2) do
      func1 = lambda {|x| x.upcase}
      func2 = lambda {|x| x+'A'}

      e = Example.new
      e.data      = (0..100).map{(97+rand(26)).chr}
      e.function << [:map, func1]
      e.function << [:map, func2]
      e.result    = e.data.map(&func1).map(&func2)
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
      func = lambda {|x| x.to_i*2}

      e = Example.new
      e.file      = file
      e.function << [:map, func]
      e.result    = data.map(&func)
      e
    end

    let(:example_2) do
      func = lambda {|x| x+"a"+"b"+"c"}

      e = Example.new
      e.file      = file
      e.function << [:map, func]
      e.result    = data.map(&func)
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

    it "3 worker" do
      example_1.workers(3).run
      example_2.workers(3).run
    end
  end

end
