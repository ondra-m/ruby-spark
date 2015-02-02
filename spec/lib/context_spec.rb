require "spec_helper"

RSpec::describe Spark::Context do

  it ".run_job" do
    workers = 5
    numbers = (0...100).to_a
    func = lambda{|part| part.size}

    rdd = $sc.parallelize(numbers, workers, batch_size: 1)

    rdd_result = $sc.run_job(rdd, func)
    result = numbers.each_slice(numbers.size/workers).map(&func)
    expect(rdd_result).to eql(result)

    parts = [0, 2]
    func = lambda{|part| part.to_s}

    rdd_result = $sc.run_job(rdd, func, parts)
    result = []
    sliced_numbers = numbers.each_slice(numbers.size/workers).to_a
    parts.each do |part|
      result << func.call(sliced_numbers[part])
    end

    expect(rdd_result).to eql(result)
  end

  it ".broadcast" do
    workers = rand(1..5)

    values1 = [1,2,3]
    values2 = [4,5,6]

    broadcast1 = $sc.broadcast(values1, 1)
    broadcast2 = $sc.broadcast(values2, 2)

    rdd = $sc.parallelize(0..5, workers)
    rdd = rdd.broadcast(broadcast1, broadcast2)
    rdd = rdd.map_partitions(lambda{|_| Broadcast[1] + Broadcast[2] })

    expect(rdd.sum).to eql(
      (values1 + values2).reduce(:+) * workers
    )
  end

  it ".accumulator" do
    accum1 = $sc.accumulator(0, 1)
    accum2 = $sc.accumulator(1, 2, :*, 1)
    accum3 = $sc.accumulator(0, 3, lambda{|max, val| val > max ? val : max})

    accum1 += 1

    accum2.add(2)
    accum2.add(2)
    accum2.add(2)

    accum3.add(9)
    accum3.add(6)
    accum3.add(7)

    expect(accum1.value).to eql(1)
    expect(accum2.value).to eql(8)
    expect(accum3.value).to eql(9)

    func = Proc.new do |_, index|
      Accumulator[1].add(1)
      Accumulator[2].add(2)
      Accumulator[3].add(index * 10)
    end

    rdd = $sc.parallelize(0..4, 4)
    rdd = rdd.accumulator(accum1, accum2).accumulator(accum3)
    rdd = rdd.map_partitions_with_index(func)
    rdd.collect

    expect(accum1.value).to eql(5)
    expect(accum2.value).to eql(128)
    expect(accum3.value).to eql(30)
  end

end
