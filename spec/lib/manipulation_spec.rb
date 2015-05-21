require 'spec_helper'

RSpec.describe 'Spark::RDD' do
  let(:numbers) { 1..100 }
  let(:rand_numbers) { Generator.numbers }

  it '.glom' do
    rdd = $sc.parallelize(numbers, 1).glom
    expect(rdd.collect).to eql([numbers.to_a])

    ser = Spark::Serializer.build { __batched__(__marshal__, 1) }

    rdd = $sc.parallelize(numbers, 5, ser).glom
    expect(rdd.collect).to eql(numbers.each_slice(20).to_a)
  end

  it '.coalesce' do
    rdd = $sc.parallelize(numbers, 5)

    rdd2 = rdd.glom
    expect(rdd2.collect.size).to eql(5)

    rdd3 = rdd.coalesce(4).glom
    expect(rdd3.collect.size).to eql(4)
  end

  it '.distinct' do
    rdd = $sc.parallelize(rand_numbers, 5)
    rdd = rdd.distinct
    expect(rdd.collect.sort).to eql(rand_numbers.uniq.sort)

    rdd = $sc.parallelize(numbers, 5)
    rdd = rdd.map(lambda{|x| 1})
    rdd = rdd.distinct
    expect(rdd.collect).to eql([1])
  end

  context '.union' do
    it 'classic method' do
      rdd = $sc.parallelize(numbers, 5)
      rdd = rdd.union(rdd).collect

      expect(rdd.collect.sort).to eql((numbers.to_a+numbers.to_a).sort)
    end

    it 'with a different serializer' do
      rdd1 = $sc.parallelize(numbers, 1, Spark::Serializer.build{ __batched__(__marshal__) })
      rdd2 = $sc.parallelize(numbers, 1, Spark::Serializer.build{ __batched__(__oj__) })

      expect { rdd1.union(rdd2).collect }.to_not raise_error
    end

    it 'as operator' do
      rdd1 = $sc.parallelize(numbers)
      rdd2 = $sc.parallelize(rand_numbers)

      expect((rdd1+rdd2).sum).to eql((numbers.to_a+rand_numbers).reduce(:+))
    end
  end

  it '.compact' do
    data = [nil, nil , 0, 0, 1, 2, nil, 6]
    result = data.compact
    ser = Spark::Serializer.build { __batched__(__marshal__, 1) }

    rdd = $sc.parallelize(data, 1).compact
    expect(rdd.collect).to eql(result)

    rdd = $sc.parallelize(data, 5, ser).compact
    expect(rdd.collect).to eql(result)

    rdd = $sc.parallelize(data, 1, ser).compact
    expect(rdd.collect).to eql(result)
  end

  it '.intersection' do
    data1 = [0,1,2,3,4,5,6,7,8,9,10]
    data2 = [5,6,7,8,9,10,11,12,13,14,15]

    rdd1 = $sc.parallelize(data1)
    rdd2 = $sc.parallelize(data2)

    expect(rdd1.intersection(rdd2).collect.sort).to eql(data1 & data2)
  end

  it '.shuffle' do
    data = Generator.numbers
    rdd = $sc.parallelize(data)

    expect(rdd.shuffle.collect).to_not eql(data)
  end

  context '.cartesian' do
    let(:data1) { Generator.numbers(100) }
    let(:data2) { Generator.numbers(100) }
    let(:result) { data1.product(data2).map(&:to_s).sort }

    it 'unbatched' do
      ser = Spark::Serializer.build { __batched__(__marshal__, 1) }

      rdd1 = $sc.parallelize(data1, 2, ser)
      rdd2 = $sc.parallelize(data2, 2, ser)

      rdd = rdd1.cartesian(rdd2).map(lambda{|x| x.to_s})

      expect(rdd.collect.sort).to eql(result)
    end

    it 'batched' do
      ser1 = Spark::Serializer.build { __batched__(__marshal__, rand(4..10)) }
      ser2 = Spark::Serializer.build { __batched__(__marshal__, rand(4..10)) }

      rdd1 = $sc.parallelize(data1, 2, ser1)
      rdd2 = $sc.parallelize(data2, 2, ser2)

      rdd = rdd1.cartesian(rdd2).map(lambda{|x| x.to_s})

      expect(rdd.collect.sort).to eql(result)
    end
  end

end
