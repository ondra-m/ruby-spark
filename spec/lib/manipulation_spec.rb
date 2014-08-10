require "spec_helper"

RSpec::describe "Spark::RDD" do
  let(:numbers) { 1..100 }
  let(:rand_numbers) { Array.new(100){ rand(1000000) } }

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

  it ".distinct" do
    rdd = $sc.parallelize(rand_numbers, 5)
    rdd = rdd.distinct
    expect(rdd.collect.sort).to eql(rand_numbers.uniq.sort)

    rdd = $sc.parallelize(numbers, 5)
    rdd = rdd.map(lambda{|x| 1})
    rdd = rdd.distinct
    expect(rdd.collect).to eql([1])
  end

  it ".union" do
    rdd = $sc.parallelize(numbers, 5)
    rdd = rdd.union(rdd).collect

    expect(rdd.collect.sort).to eql((numbers.to_a+numbers.to_a).sort)
  end

end
