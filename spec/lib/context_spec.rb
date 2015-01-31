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

end
