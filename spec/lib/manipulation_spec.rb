require "spec_helper"

RSpec::describe "Spark::RDD" do
  let(:numbers) { 1..100 }
  let(:rand_numbers) { Generator.numbers }

  it ".glom" do
    rdd = $sc.parallelize(numbers, 1).glom
    expect(rdd.collect).to eql([numbers.to_a])

    rdd = $sc.parallelize(numbers, 5, batch_size: 1).glom
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

  it ".union with a different serializer" do
    rdd1 = $sc.parallelize(numbers, 1, serializer: "marshal")
    rdd2 = $sc.parallelize(numbers, 1, serializer: "oj")
    
    expect { rdd1.union(rdd2).collect }.to_not raise_error
  end

  it ".compact" do
    data = [nil, nil , 0, 0, 1, 2, nil, 6]
    result = data.compact

    rdd = $sc.parallelize(data, 1).compact
    expect(rdd.collect).to eql(result)

    rdd = $sc.parallelize(data, 5, batch_size: 1).compact
    expect(rdd.collect).to eql(result)

    rdd = $sc.parallelize(data, 1, batch_size: 1).compact
    expect(rdd.collect).to eql(result)
  end

end
