require 'spec_helper'

RSpec.describe Spark::Config do

  before(:context) do
    Spark.stop
  end

  after(:context) do
    spark_start
  end

  it 'should be stopped' do
    expect(Spark.started?).to be_falsy
  end

  context 'new config' do

    let(:configuration) do
      {
        'test.test1' => 'test1',
        'test.test2' => 'test2',
        'test.test3' => 'test3'
      }
    end

    before(:each) do
      Spark.clear_config
    end

    it 'throught methods' do
      configuration.each do |key, value|
        Spark.config.set(key, value)
      end

      configuration.each do |key, value|
        expect(Spark.config.get(key)).to eql(value)
      end
    end

    it 'throught hash style' do
      configuration.each do |key, value|
        Spark.config[key] = value
      end

      configuration.each do |key, value|
        expect(Spark.config[key]).to eql(value)
      end
    end

    it 'throught dsl' do
      configuration.each do |key, value|
        Spark.config {
          set key, value
        }
      end

      configuration.each do |key, value|
        expect(Spark.config[key]).to eql(value)
      end
    end
  end

end
