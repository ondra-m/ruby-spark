require "spec_helper"

RSpec::shared_examples "a flat mapping" do |workers|
  it "with #{workers || 'default'} worker" do
    rdd2 = rdd(workers).map(func1)
    result = numbers.flat_map(&func1)

    expect(rdd2.collect).to eql(result)

    rdd3 = rdd(workers)
    rdd3 = rdd3.flat_map(func1)
    rdd3 = rdd3.flat_map(func2)
    rdd3 = rdd3.flat_map(func3)
    result = numbers.flat_map(&func1).flat_map(&func2).flat_map(&func3)

    expect(rdd3.collect).to eql(result)

    rdd4 = rdd(workers)
    rdd4 = rdd4.flat_map(func1)
    rdd4 = rdd4.flat_map(func2)
    rdd4 = rdd4.flat_map(func3)

    expect(rdd4.collect).to eql(rdd3.collect)
  end
end

RSpec::describe "Spark::RDD.flat_map" do
  let(:func1) { lambda{|x| x*2} }
  let(:func2) { lambda{|x| [x*3, 1, 1]} }
  let(:func3) { lambda{|x| [x*4, 2, 2]} }

  context "throught parallelize" do
    let(:numbers) { 0..10 }

    def rdd(workers)
      $sc.parallelize(numbers, workers)
    end

    it_behaves_like "a flat mapping", nil
    it_behaves_like "a flat mapping", 1
    it_behaves_like "a flat mapping", rand(10)+1
  end

  context "throught text_file" do
    let(:file) { File.join("spec", "inputs", "numbers_0_100.txt") }
    let(:numbers) { File.readlines(file).map(&:strip) }

    def rdd(workers)
      $sc.text_file(file, workers)
    end

    it_behaves_like "a flat mapping", nil
    it_behaves_like "a flat mapping", 1
    it_behaves_like "a flat mapping", rand(10)+1
  end
end
