require 'spec_helper'

RSpec.describe 'Spark::Mllib regression' do
  context 'labeled point' do
    let(:lp) { LabeledPoint.new(1, [1,2,3]) }

    it 'from array' do
      expect(lp.label).to eql(1.0)
      expect(lp.features).to be_a(DenseVector)
    end

    it 'serialize' do
      lp2 = Marshal.load(Marshal.dump(lp))

      expect(lp2.label).to eql(lp.label)
      expect(lp2.features.values).to eql(lp.features.values)
    end
  end

  context 'linear regression' do
    # Y = 3 + 10*X1 + 10*X2
    it 'linear regression' do
      data = Spark.jb.call(RubyMLLibUtilAPI, 'generateLinearInput', 3.0, ['10.0', '10.0'], 100, 42, 0.1)
      rdd = $sc.parallelize(data)

      lrm = LinearRegressionWithSGD.train(rdd, iterations: 1000, intercept: true, step: 1.0)

      expect(lrm.intercept).to be_between(2.5, 3.5)
      expect(lrm.weights.size).to eq(2)
      expect(lrm.weights[0]).to be_between(9.0, 11.0)
      expect(lrm.weights[1]).to be_between(9.0, 11.0)
    end
  end
end
