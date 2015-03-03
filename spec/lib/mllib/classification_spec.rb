require 'spec_helper'

RSpec.describe 'Spark::Mllib classification' do

  let(:data1) do
    [
      LabeledPoint.new(0.0, [1, 0, 0]),
      LabeledPoint.new(1.0, [0, 1, 1]),
      LabeledPoint.new(0.0, [2, 0, 0]),
      LabeledPoint.new(1.0, [0, 2, 1])
    ]
  end

  let(:values1) do
    data1.map do |lp|
      lp.features.values
    end
  end

  let(:rdd1) { $sc.parallelize(data1) }

  context 'logistic regression' do
    it 'test' do
      lrm = LogisticRegressionWithSGD.train(rdd1)

      expect(lrm.predict(values1[0])).to be <= 0
      expect(lrm.predict(values1[1])).to be >  0
      expect(lrm.predict(values1[2])).to be <= 0
      expect(lrm.predict(values1[3])).to be >  0
    end
  end

  context 'svm' do
    it 'test' do
      lrm = SVMWithSGD.train(rdd1)

      expect(lrm.predict(values1[0])).to be <= 0
      expect(lrm.predict(values1[1])).to be >  0
      expect(lrm.predict(values1[2])).to be <= 0
      expect(lrm.predict(values1[3])).to be >  0
    end
  end

  context 'naive bayes' do
    it 'test' do
      lrm = NaiveBayes.train(rdd1)

      expect(lrm.predict(values1[0])).to be <= 0
      expect(lrm.predict(values1[1])).to be >  0
      expect(lrm.predict(values1[2])).to be <= 0
      expect(lrm.predict(values1[3])).to be >  0
    end
  end
end
