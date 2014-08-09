require "spec_helper"

RSpec::describe "Spark::RDD" do
  let(:numbers) { 1..100 }

  it ".glom" do
    rdd = $sc.parallelize(numbers, 1).glom
    expect(rdd.collect).to eql([numbers.to_a])

    rdd = $sc.parallelize(numbers, 5).glom
    expect(rdd.collect).to eql(numbers.each_slice(20).to_a)
  end

  it ".coalesce" do
    rdd = $sc.parallelize(numbers, 5)

    rdd2 = rdd.glom
    expect(rdd2.collect.size).to eql(5)

    rdd3 = rdd.coalesce(4).glom
    expect(rdd3.collect.size).to eql(4)
  end

end
