require 'spec_helper'
require 'zlib'

RSpec.describe Spark::Serializer do
  let(:data) { [1, 'test', 2.0, [3], {key: 'value'}, :test, String] }

  it 'find' do
    expect(described_class.find('not_existed_class')).to eql(nil)

    expect(described_class.find('Marshal')).to eq(described_class::Marshal)
    expect(described_class.find('marshal')).to eq(described_class::Marshal)
    expect(described_class.find(:marshal)).to eq(described_class::Marshal)
    expect(described_class.find('batched')).to eq(described_class::Batched)
  end

  it 'find!' do
    expect { expect(described_class.find!('not_existed_class')) }.to raise_error(Spark::SerializeError)
    expect { expect(described_class.find!('marshal')) }.to_not raise_error
    expect { expect(described_class.find!('batched')) }.to_not raise_error
  end

  it 'register' do
    NewSerializer = Class.new

    expect(described_class.find('new_serializer_1')).to eql(nil)
    expect(described_class.find('new_serializer_2')).to eql(nil)
    expect(described_class.find('new_serializer_3')).to eql(nil)

    described_class.register('new_serializer_1', 'new_serializer_2', 'new_serializer_3', NewSerializer)

    expect(described_class.find('new_serializer_1')).to eql(NewSerializer)
    expect(described_class.find('new_serializer_2')).to eql(NewSerializer)
    expect(described_class.find('new_serializer_3')).to eql(NewSerializer)
  end

  it '==' do
    # One class
    marshal1 = described_class::Marshal.new
    marshal2 = described_class::Marshal.new

    expect(marshal1).to eq(marshal1)
    expect(marshal1).to eq(marshal2)

    # Two classes
    compressed1 = described_class::Compressed.new(marshal1)
    compressed2 = described_class::Compressed.new(marshal2)

    expect(compressed1).to eq(compressed1)
    expect(compressed1).to eq(compressed2)

    # Three classes
    batched1 = described_class::Batched.new(compressed1, 1)
    batched2 = described_class::Batched.new(compressed2, 1)
    batched3 = described_class::Batched.new(compressed1, 2)

    expect(batched1).to eq(batched2)
    expect(batched1).to_not eq(batched3)
  end

  context 'build' do
    let(:marshal1)    { described_class::Marshal.new }
    let(:compressed1) { described_class::Compressed.new(marshal1) }
    let(:batched1)    { described_class::Batched.new(compressed1, 1) }

    it 'block' do
      expect(described_class.build{ marshal }).to eq(marshal1)
      expect(described_class.build{ marshal }).to eq(described_class.build{ __marshal__ })
      expect(described_class.build{ compressed(marshal) }).to eq(compressed1)
      expect(described_class.build{ batched(compressed(marshal), 1) }).to eq(batched1)
    end

    it 'text' do
      expect(described_class.build('marshal')).to eq(marshal1)
      expect(described_class.build('compressed(marshal)')).to eq(compressed1)
      expect(described_class.build('batched(compressed(marshal), 1)')).to eq(batched1)
    end
  end

  it 'serialization' do
    marshal1 = described_class.build{ marshal }
    compressed1 = described_class.build{ compressed(marshal) }

    expect(marshal1.dump(data)).to eq(Marshal.dump(data))
    expect(compressed1.dump(data)).to eq(
      Zlib::Deflate.deflate(Marshal.dump(data))
    )
  end

  context 'Auto batched' do
    let(:klass) { Spark::Serializer::AutoBatched }
    let(:marshal) { Spark::Serializer::Marshal.new }
    let(:numbers) { Generator.numbers }

    it 'initialize' do
      expect { klass.new }.to raise_error(ArgumentError)
      expect { klass.new(marshal) }.to_not raise_error
      expect { klass.new(marshal, 1) }.to raise_error(Spark::SerializeError)
    end

    it 'serialization' do
      serializer1 = klass.new(marshal)
      serializer2 = klass.new(marshal, 2)

      rdd1 = Spark.sc.parallelize(numbers, 2, serializer1)
      rdd2 = Spark.sc.parallelize(numbers, 2, serializer2).map(:to_i)

      result = rdd1.collect

      expect(rdd1.serializer).to eq(serializer1)
      expect(result).to eq(numbers)
      expect(result).to eq(rdd2.collect)
    end

  end
end
