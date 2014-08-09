require "spec_helper"

RSpec::describe "Spark::RDD.coalesce" do

  context "throught parallelize" do
    let(:example_1) do
      e = Example.new
      e.data      = 0...90
      e.function << [:coalesce, 3]
      e.function << [:map_partitions, lambda{|x| x.size}]
      e.result    = e.data.each_slice(30).to_a
      e
    end

    it "default parallelism" do
      expect(example_1.rdd.collect.size).to eql(3)
    end

    it "one worker" do
      example_1.workers(1).run([example_1.data.size])
    end

    it "5 worker" do
      expect(example_1.workers(5).rdd.collect.size).to eql(3)
    end
  end

  context "throught text_file" do
    let(:file) { File.join("spec", "inputs", "numbers_1_100.txt") }
    let(:data) { File.readlines(file).map(&:strip) }

    let(:example_1) do
      func = lambda{|x| x.to_i*2}

      e = Example.new
      e.file      = file
      e.function << [:map, func]
      e.function << [:coalesce, 4]
      e.function << [:map_partitions, lambda{|x| x.size}]
      e.result    = data.map(&func).each_slice(25).to_a
      e
    end

    it "default parallelism" do
      expect(example_1.rdd.collect.size).to eql(4)
    end

    it "one worker" do
      example_1.workers(1).run([data.size])
    end

    it "8 worker" do
      expect(example_1.workers(8).rdd.collect.size).to eql(4)
    end
  end

end
