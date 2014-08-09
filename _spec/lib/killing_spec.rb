require "spec_helper"

RSpec::describe "Spark.destroy_all" do
  it "re-create master" do
    rdd = $sc.parallelize(0..5)

    result = rdd.map(lambda {|x| x*2}).collect
    expect(result).to eq([0, 2, 4, 6, 8, 10])

    Spark.destroy_all

    result = rdd.map(lambda {|x| x*2}).collect
    expect(result).to eq([0, 2, 4, 6, 8, 10])
  end
end
