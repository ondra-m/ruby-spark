require 'spec_helper'

RSpec.describe 'Spark::Mllib clustering' do
  context 'kmeans' do
    it 'test' do
      data = [
        DenseVector.new([0, 1.1]),
        DenseVector.new([0, 1.2]),
        DenseVector.new([1.1, 0]),
        DenseVector.new([1.2, 0])
      ]
      model = KMeans.train($sc.parallelize(data), 2, initialization_mode: 'k-means||')

      expect(model.predict(data[0])).to eq(model.predict(data[1]))
      expect(model.predict(data[2])).to eq(model.predict(data[3]))
    end

    it 'deterministic' do
      data = Array.new(10) do |i|
        i *= 10
        DenseVector.new([i, i])
      end

      clusters1 = KMeans.train($sc.parallelize(data), 3, initialization_mode: 'k-means||', seed: 42)
      clusters2 = KMeans.train($sc.parallelize(data), 3, initialization_mode: 'k-means||', seed: 42)

      centers1 = clusters1.centers.to_a
      centers2 = clusters2.centers.to_a

      centers1.zip(centers2).each do |c1, c2|
        expect(c1).to eq(c2)
      end
    end
  end
end
