require 'spec_helper'

RSpec.shared_examples 'a keying by' do |workers|
  it "with #{workers || 'default'} worker" do
    rdd = rdd_numbers(workers)
    rdd = rdd.key_by(key_function1)

    result = numbers.map{|item| [key_function1.call(item), item]}
    expect(rdd.collect).to eql(result)

    rdd = rdd_words(workers)
    rdd = rdd.key_by(key_function2)

    result = words.map{|item| [key_function2.call(item), item]}
    expect(rdd.collect).to eql(result)
  end
end

RSpec.describe 'Spark::RDD' do

  context 'key_by' do
    let(:key_function1) { lambda{|x| x.even?} }
    let(:key_function2) { lambda{|x| x.include?('a')} }

    let(:numbers) { Generator.numbers }
    let(:words)   { Generator.words }

    def rdd_numbers(workers)
      $sc.parallelize(numbers)
    end

    def rdd_words(workers)
      $sc.parallelize(words)
    end

    it_behaves_like 'a keying by', 1
    it_behaves_like 'a keying by', 2
    # it_behaves_like 'a keying by', nil
    # it_behaves_like 'a keying by', rand(2..10)
  end

  it 'lookup' do
    numbers = Generator.numbers
    rdd_numbers = $sc.parallelize(numbers, 2)

    rdd = rdd_numbers.group_by(lambda {|x| x%3})
    rdd.lookup(2)

    expect(rdd.lookup(2).first).to eq(
      numbers.group_by{|x| x%3}[2]
    )

    rdd = rdd_numbers.key_by(lambda{|x| x.even?})
    expect(rdd.lookup(true)).to eq(
      numbers.select(&:even?)
    )
  end

end
