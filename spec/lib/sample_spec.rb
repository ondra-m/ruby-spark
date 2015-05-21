require 'spec_helper'

# Sample method can not be tested because of random generator
# Just test it for raising error

RSpec.shared_examples 'a sampler' do |workers|
  context "with #{workers || 'default'} worker" do

    context '.sample' do
      it 'with replacement' do
        rdd2 = rdd(workers).sample(true, rand)
        expect { rdd2.collect }.to_not raise_error
      end

      it 'without replacement' do
        rdd2 = rdd(workers).sample(false, rand)
        expect { rdd2.collect }.to_not raise_error
      end
    end

    context '.take_sample' do
      it 'with replacement' do
        size = rand(10..999)
        expect(rdd(workers).take_sample(true, size).size).to eql(size)
      end

      it 'without replacement' do
        size = rand(10..999)
        expect(rdd(workers).take_sample(false, size).size).to eql(size)
      end
    end

  end
end

RSpec.describe 'Spark::RDD' do
  let(:numbers) { Generator.numbers(1000) }

  def rdd(workers)
    $sc.parallelize(numbers, workers)
  end

  it_behaves_like 'a sampler', 1
  it_behaves_like 'a sampler', 2
  # it_behaves_like 'a sampler', nil
  # it_behaves_like 'a sampler', rand(2..10)
end
