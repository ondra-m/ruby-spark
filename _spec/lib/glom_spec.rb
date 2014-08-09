require "spec_helper"

RSpec::describe "Spark::RDD.glom" do

  context "throught parallelize" do
    let(:example_1) do
      e = Example.new
      e.data      = 0...100
      e.function << [:glom]
      e
    end

    it "one worker" do
      example_1.workers(1).run([example_1.data.to_a])
    end

    it "5 worker" do
      example_1.workers(5).run(example_1.data.each_slice(20).to_a)
    end
  end

  context "throught text_file" do
    let(:file) { File.join("spec", "inputs", "numbers_1_100.txt") }
    let(:data) { File.readlines(file).map(&:strip) }

    let(:example_1) do
      e = Example.new
      e.file      = file
      e.function << [:glom]
      e
    end

    it "one worker" do
      example_1.workers(1).run([data])
    end
  end

end
