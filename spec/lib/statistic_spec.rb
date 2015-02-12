require 'spec_helper'

RSpec::shared_examples 'a stats' do |workers|
  let(:numbers) { [1.0, 1.0, 2.0, 3.0, 5.0, 8.0] }

  it "with #{workers || 'default'} worker" do
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
end

RSpec::describe Spark::RDD do

  context '.stats' do
    it_behaves_like 'a stats', 1
    it_behaves_like 'a stats', rand(2..5)
  end

end
