require 'spec_helper'

RSpec.describe Spark::Context do

  it '.run_job' do
    workers = 5
    numbers = (0...100).to_a
    func = lambda{|part| part.size}

    ser = Spark::Serializer.build { __batched__(__marshal__, 1) }

    rdd = $sc.parallelize(numbers, workers, ser)

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

  it '.broadcast' do
    workers = rand(1..5)

    values1 = [1,2,3]
    values2 = [4,5,6]

    broadcast1 = $sc.broadcast(values1)
    broadcast2 = $sc.broadcast(values2)

    rdd = $sc.parallelize(0..5, workers)
    rdd = rdd.bind(broadcast1: broadcast1, broadcast2: broadcast2)
    rdd = rdd.map_partitions(lambda{|_| broadcast1.value + broadcast2.value })

    expect(rdd.sum).to eql(
      (values1 + values2).reduce(:+) * workers
    )
  end

  # context '.accumulator' do

  #   it 'test' do
  #     accum1 = $sc.accumulator(0,)
  #     accum2 = $sc.accumulator(1, :*, 1)
  #     accum3 = $sc.accumulator(0, lambda{|max, val| val > max ? val : max})

  #     accum1 += 1

  #     accum2.add(2)
  #     accum2.add(2)
  #     accum2.add(2)

  #     accum3.add(9)
  #     accum3.add(6)
  #     accum3.add(7)

  #     expect(accum1.value).to eql(1)
  #     expect(accum2.value).to eql(8)
  #     expect(accum3.value).to eql(9)

  #     func = Proc.new do |_, index|
  #       accum1.add(1)
  #       accum2.add(2)
  #       accum3.add(index * 10)
  #     end

  #     rdd = $sc.parallelize(0..4, 4)
  #     rdd = rdd.bind(accum1: accum1, accum2: accum2, accum3: accum3)
  #     rdd = rdd.map_partitions_with_index(func)
  #     rdd.collect

  #     # http://stackoverflow.com/questions/28560133/ruby-server-java-scala-client-deadlock
  #     sleep(1)

  #     expect(accum1.value).to eql(5)
  #     expect(accum2.value).to eql(128)
  #     expect(accum3.value).to eql(30)
  #   end

  #   context 'accum param' do
  #     it 'symbol' do
  #       accum1 = $sc.accumulator(1, :+, 0)
  #       accum2 = $sc.accumulator(5, :-, 3)
  #       accum3 = $sc.accumulator(1, :*, 1)
  #       accum4 = $sc.accumulator(1.0, :/, 1.0)
  #       accum5 = $sc.accumulator(2, :**, 2)

  #       func = Proc.new do |_|
  #         accum1.add(1)
  #         accum2.add(1)
  #         accum3.add(2)
  #         accum4.add(2)
  #         accum5.add(2)
  #       end

  #       rdd = $sc.parallelize(0..4, 2)
  #       rdd = rdd.bind(accum1: accum1, accum2: accum2, accum3: accum3, accum4: accum4, accum5: accum5)
  #       rdd = rdd.map_partitions(func)
  #       rdd.collect

  #       # http://stackoverflow.com/questions/28560133/ruby-server-java-scala-client-deadlock
  #       sleep(1)

  #       expect(accum1.value).to eq(3)
  #       expect(accum2.value).to eq(1)
  #       expect(accum3.value).to eq(4)
  #       expect(accum4.value).to eq(4)
  #       expect(accum5.value).to eq(65536)
  #     end

  #     it 'proc' do
  #       accum1 = $sc.accumulator(1, lambda{|mem, val| mem + val}, 0)
  #       accum2 = $sc.accumulator('a', lambda{|mem, val| mem + val}, '')
  #       accum3 = $sc.accumulator([], lambda{|mem, val| mem << val}, [])

  #       func = Proc.new do |_|
  #         accum1.add(1)
  #         accum2.add('a')
  #         accum3.add(1)
  #       end

  #       rdd = $sc.parallelize(0..4, 2)
  #       rdd = rdd.bind(accum1: accum1, accum2: accum2, accum3: accum3)
  #       rdd = rdd.map_partitions(func)
  #       rdd.collect

  #       # http://stackoverflow.com/questions/28560133/ruby-server-java-scala-client-deadlock
  #       sleep(1)

  #       expect(accum1.value).to eq(3)
  #       expect(accum2.value).to eq('aaa')
  #       expect(accum3.value).to eq([[1], [1]])
  #     end

  #     it 'string' do
  #       expect { $sc.accumulator(1, '0') }.to raise_error(Spark::SerializeError)

  #       accum = $sc.accumulator(1, 'lambda{|mem, val| mem + val}', 0)

  #       func = Proc.new do |_|
  #         accum.add(1)
  #       end

  #       rdd = $sc.parallelize(0..4, 2)
  #       rdd = rdd.bind(accum: accum)
  #       rdd = rdd.map_partitions(func)
  #       rdd.collect

  #       # http://stackoverflow.com/questions/28560133/ruby-server-java-scala-client-deadlock
  #       sleep(1)

  #       expect(accum.value).to eq(3)
  #     end
  #   end
  # end

end
