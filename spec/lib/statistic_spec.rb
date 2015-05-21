require 'spec_helper'

RSpec.shared_examples 'a stats' do |workers|
  let(:numbers) { [1.0, 1.0, 2.0, 3.0, 5.0, 8.0] }

  context "with #{workers || 'default'} worker" do
    it 'stats class' do
      stats = $sc.parallelize(numbers, workers).stats

      expect(stats.sum).to             be_within(0.1).of(20)
      expect(stats.mean).to            be_within(0.1).of(20/6.0)
      expect(stats.max).to             be_within(0.1).of(8.0)
      expect(stats.min).to             be_within(0.1).of(1.0)
      expect(stats.variance).to        be_within(0.1).of(6.22222)
      expect(stats.sample_variance).to be_within(0.1).of(7.46667)
      expect(stats.stdev).to           be_within(0.1).of(2.49444)
      expect(stats.sample_stdev).to    be_within(0.1).of(2.73252)
    end

    it 'rdd methods' do
      rdd = $sc.parallelize([1, 2, 3], workers)

      expect(rdd.mean).to            be_within(0.1).of(2.0)
      expect(rdd.variance).to        be_within(0.1).of(0.666)
      expect(rdd.stdev).to           be_within(0.1).of(0.816)
      expect(rdd.sample_stdev).to    be_within(0.1).of(1.0)
      expect(rdd.sample_variance).to be_within(0.1).of(1.0)
    end
  end
end

RSpec.shared_examples 'a histogram' do |workers|

  context "with #{workers || 'default'} worker" do
    it 'empty' do
      rdd = $sc.parallelize([], workers, ser)

      expect( rdd.histogram([0, 10])[1] ).to eq([0])
      expect( rdd.histogram([0, 4, 10])[1] ).to eq([0, 0])
    end

    it 'validation' do
      rdd = $sc.parallelize([], workers, ser)
      expect { rdd.histogram(0) }.to raise_error(ArgumentError)
    end

    it 'double' do
      rdd = $sc.parallelize([1.0, 2.0, 3.0, 4.0], workers, ser)
      buckets, counts = rdd.histogram(2)

      expect(buckets).to eq([1.0, 2.5, 4.0])
      expect(counts).to eq([2, 2])
    end

    it 'out of range' do
      rdd = $sc.parallelize([10.01, -0.01], workers, ser)

      expect( rdd.histogram([0, 10])[1] ).to eq([0])
      expect( rdd.histogram([0, 4, 10])[1] ).to eq([0, 0])
    end

    it 'in range with one bucket' do
      rdd = $sc.parallelize([1, 2, 3, 4], workers, ser)

      expect( rdd.histogram([0, 10])[1] ).to eq([4])
      expect( rdd.histogram([0, 4, 10])[1] ).to eq([3, 1])
    end

    it 'in range with one bucket exact match' do
      rdd = $sc.parallelize([1, 2, 3, 4], workers, ser)
      expect( rdd.histogram([1, 4])[1] ).to eq([4])
    end

    it 'out of range with two buckets' do
      rdd = $sc.parallelize([10.01, -0.01], workers, ser)
      expect( rdd.histogram([0, 5, 10])[1] ).to eq([0, 0])
    end

    it 'out of range with two uneven buckets' do
      rdd = $sc.parallelize([10.01, -0.01], workers, ser)
      expect( rdd.histogram([0, 4, 10])[1] ).to eq([0, 0])
    end

    it 'in range with two buckets' do
      rdd = $sc.parallelize([1, 2, 3, 5, 6], workers, ser)
      expect( rdd.histogram([0, 5, 10])[1] ).to eq([3, 2])
    end

    it 'in range with two bucket and nil' do
      rdd = $sc.parallelize([1, 2, 3, 5, 6, nil, Float::NAN], workers, ser)
      expect( rdd.histogram([0, 5, 10])[1] ).to eq([3, 2])
    end

    it 'in range with two uneven buckets' do
      rdd = $sc.parallelize([1, 2, 3, 5, 6], workers, ser)
      expect( rdd.histogram([0, 5, 11])[1] ).to eq([3, 2])
    end

    it 'mixed range with two uneven buckets' do
      rdd = $sc.parallelize([-0.01, 0.0, 1, 2, 3, 5, 6, 11.0, 11.01], workers, ser)
      expect( rdd.histogram([0, 5, 11])[1] ).to eq([4, 3])
    end

    it 'mixed range with four uneven buckets' do
      rdd = $sc.parallelize([-0.01, 0.0, 1, 2, 3, 5, 6, 11.01, 12.0, 199.0, 200.0, 200.1], workers, ser)
      expect( rdd.histogram([0.0, 5.0, 11.0, 12.0, 200.0])[1] ).to eq([4, 2, 1, 3])
    end

    it 'mixed range with uneven buckets and NaN' do
      rdd = $sc.parallelize([-0.01, 0.0, 1, 2, 3, 5, 6, 11.01, 12.0, 199.0, 200.0, 200.1, nil, Float::NAN], workers, ser)
      expect( rdd.histogram([0.0, 5.0, 11.0, 12.0, 200.0])[1] ).to eq([4, 2, 1, 3])
    end

    it 'out of range with infinite buckets' do
      rdd = $sc.parallelize([10.01, -0.01, Float::NAN, Float::INFINITY], workers, ser)
      expect( rdd.histogram([-Float::INFINITY, 0, Float::INFINITY])[1] ).to eq([1, 1])
    end

    it 'without buckets' do
      rdd = $sc.parallelize([1, 2, 3, 4], workers, ser)
      expect( rdd.histogram(1) ).to eq([[1, 4], [4]])
    end

    it 'without buckets single element' do
      rdd = $sc.parallelize([1], workers, ser)
      expect( rdd.histogram(1) ).to eq([[1, 1], [1]])
    end

    it 'without bucket no range' do
      rdd = $sc.parallelize([1, 1, 1, 1], workers, ser)
      expect( rdd.histogram(1) ).to eq([[1, 1], [4]])
    end

    it 'without buckets basic two' do
      rdd = $sc.parallelize([1, 2, 3, 4], workers, ser)
      expect( rdd.histogram(2) ).to eq([[1, 2.5, 4], [2, 2]])
    end

    it 'without buckets with more requested than elements' do
      rdd = $sc.parallelize([1, 2], workers, ser)
      buckets = [1.0, 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 1.9, 2.0]
      hist = [1, 0, 0, 0, 0, 0, 0, 0, 0, 1]

      expect( rdd.histogram(10) ).to eq([buckets, hist])
    end

    it 'string' do
      rdd = $sc.parallelize(['ab', 'ac', 'b', 'bd', 'ef'], workers, ser)

      expect( rdd.histogram(['a', 'b', 'c'])[1] ).to eq([2, 2])
      expect( rdd.histogram(1) ).to eq([['ab', 'ef'], [5]])
      expect { rdd.histogram(2) }.to raise_error(Spark::RDDError)
    end

  end
end

RSpec.describe Spark::RDD do
  let(:ser) { Spark::Serializer.build { __batched__(__marshal__, 1) } }

  context '.stats' do
    it_behaves_like 'a stats', 1
    it_behaves_like 'a stats', 2
    # it_behaves_like 'a stats', rand(2..5)
  end

  context '.histogram' do
    it_behaves_like 'a histogram', 1
    it_behaves_like 'a histogram', 2
    # it_behaves_like 'a histogram', rand(2..5)
  end
end
