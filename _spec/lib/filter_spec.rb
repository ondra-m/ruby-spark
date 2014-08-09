require "spec_helper"

RSpec::describe "Spark::RDD.filter" do

  context "throught parallelize" do
    let(:example_1) do
      func = lambda {|x| x.even?}

      e = Example.new
      e.data      = (0..100).to_a
      e.function << [:filter, func]
      e.result    = e.data.select(&func)
      e
    end

    let(:example_2) do
      func = lambda {|x| x % 3 == 0}

      e = Example.new
      e.data      = (0..100).map{(rand*10000).to_i}
      e.function << [:filter, func]
      e.result    = e.data.select(&func)
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

    it "6 workers" do
      example_1.workers(6).run
      example_2.workers(6).run
    end
  end

  context "throught text_file" do
    let(:file) { File.join("spec", "inputs", "numbers_0_100.txt") }
    let(:data) { File.readlines(file).map(&:strip) }

    let(:example_1) do
      func = lambda {|x| x.to_i.even?}

      e = Example.new
      e.file      = file
      e.function << [:filter, func]
      e.result    = data.select(&func)
      e
    end

    let(:example_2) do
      func = lambda {|x| x.ord > 60 && x.ord < 97 }

      e = Example.new
      e.file      = file
      e.function << [:filter, func]
      e.result    = data.select(&func)
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

    it "8 workers" do
      example_1.workers(8).run
      example_2.workers(8).run
    end
  end

end
