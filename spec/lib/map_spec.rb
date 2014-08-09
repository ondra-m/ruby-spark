require "spec_helper"

RSpec::shared_examples "a mapping" do |workers|
  it "with #{workers || 'default'} worker" do
    rdd2 = rdd(workers).map(func1)
    result = numbers.map(&func1)

    expect(rdd2.collect).to eql(result)

    rdd3 = rdd(workers)
    rdd3 = rdd3.map(func1)
    rdd3 = rdd3.map(func2)
    rdd3 = rdd3.map(func3)
    result = numbers.map(&func1).map(&func2).map(&func3)

    expect(rdd3.collect).to eql(result)

    rdd4 = rdd(workers)
    rdd4 = rdd4.map(func3)
    rdd4 = rdd4.map(func2)
    rdd4 = rdd4.map(func1)

    expect(rdd4.collect).to eql(rdd3.collect)
  end
end

RSpec::describe "Spark::RDD.map" do
  let(:func1) { lambda{|x| x*2} }
  let(:func2) { lambda{|x| x*3} }
  let(:func3) { lambda{|x| x*4} }

  context "throught parallelize" do
    let(:numbers) { 0..1000 }

    def rdd(workers)
      $sc.parallelize(numbers, workers)
    end

    it_behaves_like "a mapping", nil
    it_behaves_like "a mapping", 1
    it_behaves_like "a mapping", rand(10)+1
  end

  context "throught text_file" do
    let(:file) { File.join("spec", "inputs", "numbers_0_100.txt") }
    let(:numbers) { File.readlines(file).map(&:strip) }

    def rdd(workers)
      $sc.text_file(file, workers)
    end

    it_behaves_like "a mapping", nil
    it_behaves_like "a mapping", 1
    it_behaves_like "a mapping", rand(10)+1
  end
end
