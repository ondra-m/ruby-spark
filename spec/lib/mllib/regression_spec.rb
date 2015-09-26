require 'spec_helper'

# Mllib functions are tested on Spark
# This just test if ruby call proper methods

RSpec.describe 'Spark::Mllib regression' do

  let(:data1) do
    [
      LabeledPoint.new(-1.0, [0, -1]),
      LabeledPoint.new(1.0, [0, 1]),
      LabeledPoint.new(-1.0, [0, -2]),
      LabeledPoint.new(1.0, [0, 2])
    ]
  end

  let(:values1) do
    data1.map do |lp|
      lp.features.values
    end
  end

  let(:rdd1) { $sc.parallelize(data1) }

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
    context 'test' do
      let(:lrm) { LinearRegressionWithSGD.train(rdd1) }

      it 'test' do
        expect(lrm.predict(values1[0])).to be <= 0
        expect(lrm.predict(values1[1])).to be >  0
        expect(lrm.predict(values1[2])).to be <= 0
        expect(lrm.predict(values1[3])).to be >  0
      end

      it 'test via rdd' do
        rdd = $sc.parallelize(values1, 1)
        rdd = rdd.map(lambda{|value| model.predict(value)})
        rdd = rdd.bind(model: lrm)

        result = rdd.collect

        expect(result[0]).to be <= 0
        expect(result[1]).to be >  0
        expect(result[2]).to be <= 0
        expect(result[3]).to be >  0
      end
    end

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

  context 'lasso' do
    it 'test' do
      lrm = LassoWithSGD.train(rdd1)

      expect(lrm.predict(values1[0])).to be <= 0
      expect(lrm.predict(values1[1])).to be >  0
      expect(lrm.predict(values1[2])).to be <= 0
      expect(lrm.predict(values1[3])).to be >  0
    end

    it 'local random SGD with initial weights' do
      data = Spark.jb.call(RubyMLLibUtilAPI, 'generateLinearInput', 2.0, ['-1.5', '0.01'], 1000, 42, 0.1)
      data.map! do |lp|
        LabeledPoint.new(lp.label, [1.0] + lp.features.values)
      end

      rdd = $sc.parallelize(data);

      lrm = LassoWithSGD.train(rdd, step: 1.0, reg_param: 0.01, iterations: 40, initial_weights: [-1.0, -1.0, -1.0])

      expect(lrm.weights[0]).to be_between(1.9, 2.1)
      expect(lrm.weights[1]).to be_between(-1.60, -1.40)
      expect(lrm.weights[2]).to be_between(-1.0e-2, 1.0e-2)
    end
  end

  context 'ridge' do
    it 'test' do
      lrm = RidgeRegressionWithSGD.train(rdd1)

      expect(lrm.predict(values1[0])).to be <= 0
      expect(lrm.predict(values1[1])).to be >  0
      expect(lrm.predict(values1[2])).to be <= 0
      expect(lrm.predict(values1[3])).to be >  0
    end
  end
end
