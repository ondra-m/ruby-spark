require 'spec_helper'

RSpec.describe Spark::RDD do

  context '.pipe' do
    let(:words)   { Generator.words }
    let(:numbers) { Generator.numbers }

    it 'single program' do
      skip if windows?

      rdd = $sc.parallelize(words, 1)
      rdd = rdd.pipe('tr a b')

      result = words.dup
      result.map! do |x|
        x.gsub('a', 'b')
      end

      expect(rdd.collect).to eql(result)
    end

    it 'multiple program' do
      skip if windows?

      rdd = $sc.parallelize(numbers, 1)
      rdd = rdd.pipe("tr 1 5", "awk '{print $1*10}'")
      rdd = rdd.map(lambda{|x| x.to_i * 100})

      result = numbers.dup
      result.map! do |x|
        x.to_s.gsub('1', '5')
      end
      result.map! do |x|
        x.to_i * 10
      end
      result.map! do |x|
        x * 100
      end

      expect(rdd.collect).to eql(result)
    end
  end

end
