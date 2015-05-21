require 'spec_helper'

RSpec.shared_examples 'a groupping by key' do |workers|
  it "with #{workers || 'default'} worker" do
    expect(rdd_result(workers)).to eql(result)
  end
end

RSpec.shared_examples 'a cogroupping by key' do |workers|
  context "with #{workers || 'default'} worker" do
    it '.group_with' do
      rdd = rdd_1(workers).group_with(rdd_2(workers))
      expect(rdd.collect_as_hash).to eql(result_12)
    end

    it '.cogroup' do
      rdd = rdd_1(workers).cogroup(rdd_2(workers), rdd_3(workers))
      expect(rdd.collect_as_hash).to eql(result_123)
    end
  end
end

RSpec.shared_examples 'a groupping by' do |workers|
  it "with #{workers || 'default'} worker" do
    rdd = rdd_numbers(workers)
    rdd = rdd.group_by(key_function1)

    expect(rdd.collect_as_hash).to eql(numbers.group_by(&key_function1))

    rdd = rdd_words(workers)
    rdd = rdd.group_by(key_function2)

    expect(rdd.collect_as_hash).to eql(words.group_by(&key_function2))
  end
end

RSpec.describe 'Spark::RDD' do

  def make_result(*hashes)
    _result = {}
    hashes.each do |data|
      data.each do |key, value|
        _result[key] ||= []
        _result[key] << value
      end
    end
    _result
  end

  context '.group_by_key' do
    let(:hash) { Generator.hash }
    let(:result) { make_result(hash) }

    def rdd_result(workers)
      rdd = $sc.parallelize(hash)
      rdd.group_by_key.collect_as_hash
    end

    it_behaves_like 'a groupping by key', 1
    it_behaves_like 'a groupping by key', 2
    # it_behaves_like 'a groupping by key', nil
    # it_behaves_like 'a groupping by key', rand(2..10)
  end

  context 'cogroup' do
    let(:hash1) { Generator.hash }
    let(:hash2) { Generator.hash }
    let(:hash3) { Generator.hash }

    let(:result_12)  { make_result(hash1, hash2) }
    let(:result_123) { make_result(hash1, hash2, hash3) }

    def rdd_1(workers)
      $sc.parallelize(hash1)
    end

    def rdd_2(workers)
      $sc.parallelize(hash2)
    end

    def rdd_3(workers)
      $sc.parallelize(hash3)
    end

    it_behaves_like 'a cogroupping by key', 1
    it_behaves_like 'a cogroupping by key', 2
    # it_behaves_like 'a cogroupping by key', nil
    # it_behaves_like 'a cogroupping by key', rand(2..10)
  end

  context 'group_by' do
    let(:key_function1) { lambda{|x| x%2} }
    let(:key_function2) { lambda{|x| x.size} }

    let(:numbers) { Generator.numbers }
    let(:words)   { Generator.words }

    def rdd_numbers(workers)
      $sc.parallelize(numbers)
    end

    def rdd_words(workers)
      $sc.parallelize(words)
    end

    it_behaves_like 'a groupping by', 1
    it_behaves_like 'a groupping by', 2
    # it_behaves_like 'a groupping by', nil
    # it_behaves_like 'a groupping by', rand(2..10)
  end

end
